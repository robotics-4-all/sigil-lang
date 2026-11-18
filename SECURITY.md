# Security Policy

## Supported Versions

Sigil is in active development. Security fixes are applied to the latest
released minor version on the `main` branch. We do not currently maintain
backports for older minor releases.

| Version  | Supported |
|----------|-----------|
| `0.1.x`  | Yes       |
| `< 0.1`  | No        |

## Reporting a Vulnerability

If you discover a security vulnerability in Sigil, please **do not open a
public GitHub issue**. Instead, report it privately so we can investigate
and ship a fix before details become public.

Preferred channels (in order):

1. Open a private security advisory via GitHub:
   <https://github.com/robotics-4-all/sigil-lang/security/advisories/new>
2. If you cannot use GitHub, contact the maintainers directly via the
   contact information listed in the repository's `CODEOWNERS` file or
   `pyproject.toml` `[project].authors` section.

When reporting, please include:

- A description of the vulnerability and its potential impact.
- Reproduction steps or a minimal proof-of-concept (`.sigil` file,
  generated code snippet, or runtime trace).
- Affected versions (commit hash if possible).
- Any suggested mitigations or fixes you have in mind.

## What to Expect

- We aim to acknowledge new reports within **5 working days**.
- We aim to provide an initial assessment (severity, scope, expected
  remediation timeline) within **15 working days** of acknowledgment.
- For confirmed vulnerabilities we will coordinate a private fix, prepare a
  release, and only then publish a public advisory crediting the reporter
  (unless anonymity is requested).

## Scope

In-scope components:

- The `sigil` Python package (parser, semantics, code generator,
  expression compiler, CLI).
- The Jinja2 templates that produce generated pipelines (`sigil/templates/`).
- The behavior of generated code with respect to user-supplied `.sigil`
  inputs (e.g. expression compilation safety, schema validation, retry,
  DLQ envelope handling).

Out of scope:

- Vulnerabilities in transitive runtime dependencies (`commlib-py`,
  `textX`, `Jinja2`, optional ML runtimes). Please report those upstream;
  we will track and ship dependency upgrades when fixes are available.
- Misconfiguration of brokers (MQTT, Redis, AMQP, Kafka), the user's
  network, or the deployment environment.
- Security of the separately-maintained visual designer
  (`sigil-designer` repository).

## Safe Harbour

We will not pursue legal action against researchers who:

- Make a good-faith effort to avoid privacy violations, data destruction,
  and service interruption while researching.
- Report vulnerabilities through one of the channels listed above.
- Do not exploit vulnerabilities beyond what is necessary to demonstrate
  the issue.

Thank you for helping keep Sigil and its users safe.
