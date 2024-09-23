# alice's highly opinionated Node.js + TypeScript + Bun + ESLint + Prettier + Pino starter repo (fall 2024 edition)

This repo is a starting point for new projects. It includes a basic setup for a Node.js project with TypeScript, ESLint, Prettier, and Pino.

## Known issues

ESLint will throw the following error when running `bun lint`:

```
  0:0  error  Parsing error: "parserOptions.project" has been provided for @typescript-eslint/parser.
The file was not found in any of the provided project(s): eslint.config.mjs
```

If you use this repo and find a way to fix this, please do open a PR, I've spent way too long on this already. It looks simple, but it's not. In the meantime I've simply made my peace with it.
