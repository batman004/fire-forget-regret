"""Act 1: fire and forget, then the caller goes away.

A request handler starts a background task and returns. When the process
(here: the event loop) shuts down, pending tasks are cancelled. Nothing is
logged and nothing is retried. The email just never goes out.
"""

import asyncio


async def send_welcome_email(user: str) -> None:
    print(f"  task:    sending welcome email to {user}...", flush=True)
    await asyncio.sleep(1)  # talking to the SMTP server
    print(f"  task:    ✓ welcome email sent to {user}", flush=True)  # never printed


async def handle_signup(user: str) -> None:
    asyncio.create_task(send_welcome_email(user))  # fire and forget
    await asyncio.sleep(0.1)
    print("  handler: 201 Created, returning", flush=True)


def main() -> None:
    asyncio.run(handle_signup("ada@example.com"))
    print("  process: exited. Was the email sent? Nothing says it wasn't.", flush=True)


if __name__ == "__main__":
    main()
