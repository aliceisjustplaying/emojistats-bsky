# Contributing

This is a short guide to help you get set up and ready to contribute to this project.

## Setting up a local environment

1. (Fork and) clone the repo.
2. Ensure you have a global installation of `bun`, `vite` and `concurrently`.
3. Run `bun i` from the repo root to install all the dependencies.
4. Set up a [Redis dev server](https://redis.io/docs/latest/operate/oss_and_stack/install/install-redis/) (it doesn't take long at all!)
5. Ensure you are running a node version with ws support (v21+)
6. Start the dev server with `bun run dev` from the repo root.
7. You should now see streaming emojis in the terminal, and the frontend should be available at `http://localhost:5173`.
