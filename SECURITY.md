# Security

## Supported versions

Only the latest 0.x release gets security fixes.

## Reporting a vulnerability

Please report privately, not in a public issue. Use GitHub's private vulnerability reporting: open the repository's
Security tab and press "Report a vulnerability".

Please include:

- the kharka version (`kharka --version`) and the operating system;
- which part is affected: daemon, hub, CLI or the socket;
- what you did, what you expected and what happened, with the smallest steps or input that show it;
- what an attacker gains from it, if you know.

kharka is a one-person project. I reply as time allows, and I cannot promise a deadline. I will tell you whether I
consider the report a vulnerability, and I will credit you in the fix if you want that.

## Security model

In short; the details are in [docs/design.md](docs/design.md).

- **ssh is the transport and the authentication.** The hub opens no port. ssh and its keys decide who can reach it.
- **Anyone who can reach the hub's socket is trusted.** The hub only checks that a node stamps its writes with its own
  name. It does not stop a node from lying about what it holds.
- **`--sync` scopes are not access control.** A daemon may ask for any prefixes; the scope only saves space and traffic.
- **The daemon serves only its own user.** The local socket is 0600 and the data folders are 0700.
- **Values are not encrypted at rest.** The log and snapshot hold them as plain JSON; protect the disk and the account.
