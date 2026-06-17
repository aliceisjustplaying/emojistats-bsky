use std::cmp;

use reqwest::{Client, Response, StatusCode};

use super::{
    CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES, ClickHouseClientConfig, ClickHouseInsertContext,
    ClickHouseInsertError, ClickHouseInsertPayload, ClickHouseInsertReceipt, ClickHouseSqlContext,
    ClickHouseSqlError, ClickHouseSqlReceipt,
};

/// Execute insert payloads in order through the provided HTTP client.
///
/// # Errors
///
/// Returns [`ClickHouseInsertError`] when a request cannot be built, the HTTP transport fails, or
/// `ClickHouse` returns a non-2xx status.
pub async fn execute_insert_payloads(
    client: &Client,
    config: &ClickHouseClientConfig,
    payloads: &[ClickHouseInsertPayload],
) -> Result<Vec<ClickHouseInsertReceipt>, ClickHouseInsertError> {
    let mut receipts = Vec::with_capacity(payloads.len());

    for payload in payloads {
        receipts.push(execute_insert_payload_with_retries(client, config, payload).await?);
    }

    Ok(receipts)
}

/// Execute SQL statements in order through the provided HTTP client.
///
/// # Errors
///
/// Returns [`ClickHouseSqlError`] when a request cannot be built, the HTTP transport fails, or
/// `ClickHouse` returns a non-2xx status.
pub async fn execute_sql_statements(
    client: &Client,
    config: &ClickHouseClientConfig,
    statements: &[String],
) -> Result<Vec<ClickHouseSqlReceipt>, ClickHouseSqlError> {
    let mut receipts = Vec::with_capacity(statements.len());

    for (statement_index, statement) in statements.iter().enumerate() {
        receipts.push(
            execute_sql_statement_with_retries(client, config, statement_index, statement).await?,
        );
    }

    Ok(receipts)
}

async fn execute_insert_payload_with_retries(
    client: &Client,
    config: &ClickHouseClientConfig,
    payload: &ClickHouseInsertPayload,
) -> Result<ClickHouseInsertReceipt, ClickHouseInsertError> {
    let context = ClickHouseInsertContext::from_payload(payload);
    let mut attempt = 1_u8;
    let mut backoff = config.retry_initial_backoff;
    let max_attempts = cmp::max(1, config.max_insert_attempts);

    loop {
        match execute_insert_payload_once(client, config, payload, &context).await {
            Ok(receipt) => return Ok(receipt),
            Err(error) if should_retry_insert_error(&error) && attempt < max_attempts => {
                tokio::time::sleep(backoff).await;
                attempt = attempt.checked_add(1).unwrap_or(max_attempts);
                backoff = cmp::min(backoff.saturating_mul(2), config.retry_max_backoff);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn execute_insert_payload_once(
    client: &Client,
    config: &ClickHouseClientConfig,
    payload: &ClickHouseInsertPayload,
    context: &ClickHouseInsertContext,
) -> Result<ClickHouseInsertReceipt, ClickHouseInsertError> {
    let request = config
        .insert_request_with_client(client, payload)
        .map_err(|source| ClickHouseInsertError::RequestBuild {
            context: context.clone(),
            source,
        })?;
    let response =
        client
            .execute(request)
            .await
            .map_err(|source| ClickHouseInsertError::Transport {
                context: context.clone(),
                source,
            })?;
    let status = response.status();
    let response_snippet =
        response_snippet(response)
            .await
            .map_err(|source| ClickHouseInsertError::Transport {
                context: context.clone(),
                source,
            })?;

    if !status.is_success() {
        return Err(classify_insert_status(
            context.clone(),
            status,
            response_snippet,
        ));
    }

    Ok(ClickHouseInsertReceipt {
        context: context.clone(),
        status: status.as_u16(),
        response_snippet,
    })
}

fn should_retry_insert_error(error: &ClickHouseInsertError) -> bool {
    match error {
        ClickHouseInsertError::Transport { source, .. } => {
            source.is_timeout() || source.is_connect() || source.is_body()
        }
        ClickHouseInsertError::RetryableStatus { .. } => true,
        ClickHouseInsertError::RequestBuild { .. }
        | ClickHouseInsertError::PermanentStatus { .. } => false,
    }
}

async fn execute_sql_statement_with_retries(
    client: &Client,
    config: &ClickHouseClientConfig,
    statement_index: usize,
    statement: &str,
) -> Result<ClickHouseSqlReceipt, ClickHouseSqlError> {
    let context = ClickHouseSqlContext {
        statement_index,
        statement: statement.to_owned(),
    };
    let mut attempt = 1_u8;
    let mut backoff = config.retry_initial_backoff;
    let max_attempts = cmp::max(1, config.max_insert_attempts);

    loop {
        match execute_sql_statement_once(client, config, &context).await {
            Ok(receipt) => return Ok(receipt),
            Err(error) if should_retry_sql_error(&error) && attempt < max_attempts => {
                tokio::time::sleep(backoff).await;
                attempt = attempt.checked_add(1).unwrap_or(max_attempts);
                backoff = cmp::min(backoff.saturating_mul(2), config.retry_max_backoff);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn execute_sql_statement_once(
    client: &Client,
    config: &ClickHouseClientConfig,
    context: &ClickHouseSqlContext,
) -> Result<ClickHouseSqlReceipt, ClickHouseSqlError> {
    let request = config
        .sql_request_with_client(client, context.statement.as_str())
        .map_err(|source| ClickHouseSqlError::RequestBuild {
            statement_index: context.statement_index,
            source,
        })?;
    let response =
        client
            .execute(request)
            .await
            .map_err(|source| ClickHouseSqlError::Transport {
                context: context.clone(),
                source,
            })?;
    let status = response.status();
    let response_snippet =
        response_snippet(response)
            .await
            .map_err(|source| ClickHouseSqlError::Transport {
                context: context.clone(),
                source,
            })?;

    if !status.is_success() {
        return Err(classify_sql_status(
            context.clone(),
            status,
            response_snippet,
        ));
    }

    Ok(ClickHouseSqlReceipt {
        context: context.clone(),
        status: status.as_u16(),
        response_snippet,
    })
}

fn should_retry_sql_error(error: &ClickHouseSqlError) -> bool {
    match error {
        ClickHouseSqlError::Transport { source, .. } => {
            source.is_timeout() || source.is_connect() || source.is_body()
        }
        ClickHouseSqlError::RetryableStatus { .. } => true,
        ClickHouseSqlError::RequestBuild { .. } | ClickHouseSqlError::PermanentStatus { .. } => {
            false
        }
    }
}

async fn response_snippet(mut response: Response) -> Result<Option<String>, reqwest::Error> {
    let mut bytes = Vec::with_capacity(CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES);
    while bytes.len() < CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        let remaining = CLICKHOUSE_RESPONSE_SNIPPET_MAX_BYTES.saturating_sub(bytes.len());
        let take = cmp::min(remaining, chunk.len());
        bytes.extend(chunk.iter().take(take).copied());
    }

    let snippet = String::from_utf8_lossy(&bytes).into_owned();
    if snippet.is_empty() {
        Ok(None)
    } else {
        Ok(Some(snippet))
    }
}

pub(super) fn classify_insert_status(
    context: ClickHouseInsertContext,
    status: StatusCode,
    response_snippet: Option<String>,
) -> ClickHouseInsertError {
    if is_retryable_insert_status(status) {
        ClickHouseInsertError::RetryableStatus {
            context,
            status: status.as_u16(),
            response_snippet,
        }
    } else {
        ClickHouseInsertError::PermanentStatus {
            context,
            status: status.as_u16(),
            response_snippet,
        }
    }
}

pub(super) fn classify_sql_status(
    context: ClickHouseSqlContext,
    status: StatusCode,
    response_snippet: Option<String>,
) -> ClickHouseSqlError {
    if is_retryable_insert_status(status) {
        ClickHouseSqlError::RetryableStatus {
            context,
            status: status.as_u16(),
            response_snippet,
        }
    } else {
        ClickHouseSqlError::PermanentStatus {
            context,
            status: status.as_u16(),
            response_snippet,
        }
    }
}

fn is_retryable_insert_status(status: StatusCode) -> bool {
    status == StatusCode::REQUEST_TIMEOUT
        || status == StatusCode::CONFLICT
        || status == StatusCode::TOO_EARLY
        || status == StatusCode::TOO_MANY_REQUESTS
        || status.is_server_error()
}
