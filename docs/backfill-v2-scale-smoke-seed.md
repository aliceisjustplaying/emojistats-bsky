# Rust v2 Scale Smoke Seed

Initial mixed DID set for testing whether the Rust v2 backfill path scales before a wider canary.

## Whales

| DID | Observed size |
| --- | ---: |
| `did:plc:o6ggjvnj4ze3mnrpnv5oravg` | 5,033,984 |
| `did:plc:f4z2nftgrn75h7h3wucdyzaf` | 4,080,764 |
| `did:plc:4hm6gb7dzobynqrpypif3dck` | 2,598,565 |
| `did:plc:lb7v3uq23wmm5spv2a27kehv` | 2,512,916 |
| `did:plc:ndjyym3ihrqu3g25gdztefrv` | 2,258,662 |

## Small Normal

| DID | Observed size |
| --- | ---: |
| `did:plc:2222n533dmpghct6kahpvjhl` | 20 |
| `did:plc:2222c6gfjmvfa52zcgsqwt2e` | 41 |
| `did:plc:2222ssnpzrr42y5d75qua7yi` | 86 |

## Empty-ish

- `did:plc:22225iagtnvhg3fi3qenypjp`
- `did:plc:22227kzdtm2zjnkmm2tiamhr`
- `did:plc:22223nnplzlrxohkjvzmr4yl`

## Account-State And Failure Cases

| Case | DID |
| --- | --- |
| Deactivated | `did:plc:222bydcfkvxgmvj5iqjvkzah` |
| Takendown | `did:plc:22223gqtyfos6p6eaz2d7tmk` |
| PLC-only failed | `did:plc:22223e4h5ivnofzvqqfmewr6` |
| Quarantined malformed `CAR` | `did:plc:22mpjou6uxsib6vs67eeq5q2` |
| Unreachable dead host | `did:plc:22227kqq2ya2bzfl22rqfgr5` |

## Same-Host Cluster: morel

- `did:plc:2b5pvtgokevj3kraceuagdng`
- `did:plc:35dnwevzhsn3wmuaqisor3dq`
- `did:plc:5yjqdpnjnhiz3zblbroeke45`
- `did:plc:gbud3onrtw2ttjyndgfce6k4`

## Same-Host Cluster: lionsmane

- `did:plc:rtty7edvocik3ps3vkneqpmu`
- `did:plc:qaz2xg3ia2v4uxonpvr4fco4`
- `did:plc:q3soic5jop3yxukyl5qmacce`
- `did:plc:t3bmctdc2rutnfarqt2zipt6`

