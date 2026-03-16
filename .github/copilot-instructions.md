# Copilot Instructions

Use these rules for work in this repository.

- Prefer reuse when it keeps the code direct. Do not introduce extra indirection, wrappers, or abstraction layers just to reuse a small amount of logic.
- Adjust naming across the system when it improves clarity. Favor consistent, explicit names over preserving unclear legacy names.
- When the user asks for an adjustment, add comments that explain why the code is structured that way when the reason is not obvious. Document why normalization exists, why stages are separated, or why a workflow boundary matters.
- If you want to change code that already has a comment explaining why it exists, ask the user whether that reason is still relevant before removing or replacing it.
- For database schema changes, prefer offline migration scripts or rebuilding the local database. Do not add runtime or startup migrations to the application code.
- Do not keep backward-compatibility shims. Prefer clearer, larger coordinated changes over temporary adapters or alias layers.