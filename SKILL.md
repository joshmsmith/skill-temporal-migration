---
name: temporal-migration
description: Migrate to Temporal from BPM, workflow, job scheduling, and low-code automation tools. Use when the user asks to "migrate from Camunda", "replace Control-M with Temporal", "migrate Airflow DAG to Temporal", "BPMN to Temporal", "migrate from TIBCO", "replace TIBCO BusinessWorks", "Pega to Temporal", "Appian to Temporal", "N8n to Temporal", "replace Tidal", "Talon to Temporal", "Quartz to Temporal", "replace job scheduler", "migrate BPM process to Temporal", "how do I replace [workflow tool] with Temporal", or asks about moving existing workflows or processes to Temporal from another orchestration platform.
version: 0.1.0
---

# Skill: temporal-migration

## Overview

This skill guides developers migrating to Temporal from other workflow orchestration, BPM, job scheduling, and low-code automation tools. It covers conceptual translation (mapping existing constructs to Temporal equivalents), migration strategy (greenfield vs. strangler-fig), and language-specific code examples.

**Supported source tools:**
- **BPMN-based BPM**: Camunda 7, Camunda 8 / Zeebe, Pega BPM, Appian, TIBCO BPM Enterprise
- **EAI / Integration platforms**: TIBCO BusinessWorks 5.x / 6.x
- **Enterprise job schedulers**: Control-M (BMC), Tidal Automation, Talon, Quartz Scheduler
- **Low-code / no-code automation**: N8n
- **DAG-based pipeline orchestrators**: Apache Airflow 2.x

> **SDK implementation details** — writing workflow code, activities, workers, error handling, versioning, signals, queries — are outside the scope of this skill. Once you understand the migration mapping, refer to the `temporal-developer` skill for SDK guidance.

## How to Use This Skill

### Step 1: Understand the Mental Model Shift

Always read this first, regardless of source tool. The single biggest cause of failed Temporal migrations is carrying over the wrong mental model.

Read **`references/core/mental-model.md`** to understand the fundamental paradigm shift from config-driven / graph-based / visual tools to Temporal's code-first durable execution model.

### Step 2: Read the Source-Tool-Specific Reference

Based on what the user is migrating FROM, read the appropriate reference:

| Source Tool | Reference File |
|---|---|
| Camunda 7, Camunda 8 / Zeebe | `references/core/from-bpmn.md` |
| Pega BPM, Appian | `references/core/from-bpmn.md` |
| TIBCO BPM Enterprise, jBPM | `references/core/from-bpmn.md` |
| TIBCO BusinessWorks 5.x / 6.x | `references/core/from-tibco-bw.md` |
| Control-M, Tidal, Talon | `references/core/from-job-schedulers.md` |
| Quartz Scheduler | `references/core/from-job-schedulers.md` |
| N8n | `references/core/from-low-code.md` |
| Apache Airflow | `references/core/from-airflow.md` |
| Unknown / general | `references/core/universal-mapping.md` |

### Step 3: Choose and Plan the Migration Strategy

Read **`references/core/migration-strategy.md`** to choose the right approach:
- **Greenfield**: Rewrite workflow logic in Temporal; decommission the old tool once done
- **Strangler-fig**: Incrementally route new work to Temporal while existing workflows finish in the legacy system
- **Parallel run**: Run both systems for the same jobs to validate parity before cutover

### Step 4: Read Language-Specific Migration Examples

After understanding the conceptual mapping, read the language-specific examples for concrete code translation patterns:

| Language | Reference File |
|---|---|
| Python | `references/python/examples.md` |
| TypeScript / JavaScript | `references/typescript/examples.md` |
| Java | `references/java/examples.md` |
| Go | `references/go/examples.md` |
| .NET / C# | `references/dotnet/examples.md` |

### Step 5: Avoid Common Pitfalls

Read **`references/core/gotchas.md`** for anti-patterns that commonly appear when migrating from specific tool categories. These are the mistakes most likely to create subtle bugs or re-introduce the same problems Temporal is meant to solve.

## Quick Reference: Concept Translation

For a fast overview of how concepts translate across ALL tools at once, see **`references/core/universal-mapping.md`**.

## Primary References

- **`references/core/mental-model.md`** — The paradigm shift from graph/config-driven to code-first durable execution
- **`references/core/universal-mapping.md`** — Master concept translation table across all source tools
- **`references/core/migration-strategy.md`** — Greenfield vs. strangler-fig, parallel-run validation, in-flight cutover
- **`references/core/from-bpmn.md`** — BPMN tools: Camunda, Pega, Appian, TIBCO BPM
- **`references/core/from-tibco-bw.md`** — TIBCO BusinessWorks EAI/integration platform
- **`references/core/from-job-schedulers.md`** — Control-M, Tidal, Talon, Quartz
- **`references/core/from-low-code.md`** — N8n
- **`references/core/from-airflow.md`** — Apache Airflow 2.x
- **`references/core/gotchas.md`** — Anti-patterns and common migration mistakes
- **`references/{language}/examples.md`** — Language-specific migration code examples

## Feedback

If this skill's explanations are unclear, missing important migration patterns, or a source tool is not covered, encourage the user to open an issue on this skill's repository.
