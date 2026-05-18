# Temporal Migration Skill

A skill for AI coding agents to guide developers migrating to [Temporal](https://temporal.io/) from other workflow orchestration, BPM, job scheduling, and low-code automation tools.

> **Status**: Public Preview. Feedback welcome — please open an issue if a migration scenario is missing or incorrect.

## Covered Source Tools

| Category | Tools |
|---|---|
| BPMN / BPM | Camunda 7, Camunda 8 (Zeebe), Pega BPM, Appian, TIBCO BPM Enterprise |
| EAI / Integration | TIBCO BusinessWorks 5.x / 6.x |
| Job Schedulers | Control-M (BMC), Tidal Automation, Talon, Quartz Scheduler |
| Low-code / no-code | N8n |
| DAG orchestrators | Apache Airflow 2.x |

## Installation

### Via `npx skills` (supports all major coding agents)

```
npx skills add your-username/skill-temporal-migration
```

Follow the prompts to select your coding agent.

### Via git clone

**Claude Code:**
```bash
mkdir -p ~/.claude/skills && git clone https://github.com/your-username/skill-temporal-migration ~/.claude/skills/temporal-migration
```

**Cursor:**
```bash
mkdir -p ~/.cursor/skills && git clone https://github.com/your-username/skill-temporal-migration ~/.cursor/skills/temporal-migration
```

**VS Code / GitHub Copilot:**
```bash
mkdir -p ~/.agents/skills && git clone https://github.com/your-username/skill-temporal-migration ~/.agents/skills/temporal-migration
```

Adjust the installation path to match your agent's skill directory convention.

## Usage

Once installed, the skill activates automatically when you describe a migration scenario, for example:

- *"I want to migrate our Camunda 7 process to Temporal in Java"*
- *"Help me replace Control-M with Temporal"*
- *"How do I migrate this Airflow DAG to Temporal in Python?"*
- *"We're moving from TIBCO BusinessWorks to Temporal"*
- *"Replace our N8n workflow with Temporal"*

For SDK implementation details after the conceptual migration is clear, also install the [`temporal-developer`](https://github.com/temporalio/skill-temporal-developer) skill.

## Structure

```
SKILL.md                            ← Skill entry point (read by the agent)
references/
  core/
    mental-model.md                 ← Paradigm shift: config/graph → code-first
    universal-mapping.md            ← Master concept translation table
    migration-strategy.md           ← Greenfield vs. strangler-fig vs. parallel-run
    from-bpmn.md                    ← Camunda, Pega, Appian, TIBCO BPM
    from-tibco-bw.md                ← TIBCO BusinessWorks (EAI/integration)
    from-job-schedulers.md          ← Control-M, Tidal, Talon, Quartz
    from-low-code.md                ← N8n
    from-airflow.md                 ← Apache Airflow 2.x
    gotchas.md                      ← Anti-patterns and common mistakes
  python/
    examples.md                     ← Python migration code examples
  typescript/
    examples.md                     ← TypeScript migration code examples
  java/
    examples.md                     ← Java migration code examples
  go/
    examples.md                     ← Go migration code examples
  dotnet/
    examples.md                     ← .NET / C# migration code examples
```

## Contributing

Contributions are welcome — especially for migration patterns and source tools not yet covered. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

MIT
