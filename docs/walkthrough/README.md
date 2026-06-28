# OSS Pulse — 完整讲解 walkthrough

这份 walkthrough 是写给**只读过项目 README,但还不知道每个文件为什么存在的你**——目标是看完每一章后,你能在面试官面前完整、有逻辑地讲出这个项目的每一段。

## 读这份文档的姿势

1. **按顺序读。** 后面的章节依赖前面的概念,跳读会断链。
2. **每章读完先合上,自己复述一遍。** 复述比划线管用一百倍。
3. **每章末尾的 "Interview script" 章节,出声练习。** 面试是嘴上说的,不是心里默念的。
4. **代码段一律打开 repo 里对应文件对照看。** 不要光看 walkthrough 里的片段。

## 章节顺序

| 章 | 标题 | 对应 Sprint / ADR |
|---|------|------------------|
| [00](00-overview.md) | 项目鸟瞰 + 七个 senior signal + 概念预热 | 全局 |
| [01](01-sprint0-schema-discovery.md) | Sprint 0 — Schema 探索为什么必须先做 | ADR-0001 |
| [02](02-sprint1-bronze-ingestion.md) | Sprint 1 — Bronze 摄入 + 幂等性 + 分区 | ADR-0002, 0003 |
| [03](03-sprint1-first-silver.md) | Sprint 1 step 4 — 第一个 Silver 模型 + dbt 入门 | — |
| [04](04-sprint2-first-gold-mart.md) | Sprint 2 — 第一个 Gold mart + composite key | ADR-0004 |
| [05](05-sprint2.5-bot-spike.md) | Sprint 2.5 — 为什么要做 spike + 真实负发现 | 为 ADR-0006 铺路 |
| [06](06-sprint3-marts-2-and-3.md) | Sprint 3 — OSS Health + Bot Mart + cross-mart 抓 bug | ADR-0005, 0006 |
| [07](07-sprint4-dq-airflow-runbooks.md) | Sprint 4 — 自研 DQ gate + Airflow + 4 份 runbook | — |
| [08](08-sprint5b-ci-and-perf.md) | Sprint 5b 上半 — GitHub Actions CI + 性能调优诚实负结果 | ADR-0007 |
| [09](09-sprint5b-incident-postmortem.md) | Sprint 5b 下半 — 故意制造事故 + 5 Whys postmortem | — |
| [10](10-sprint6-streaming-mvp.md) | Sprint 6 — Redpanda + Structured Streaming + 0 行差 reconcile | — |
| [11](11-sprint5a-cloud-migration.md) | Sprint 5a — Terraform + S3 真上云(以及没上的部分) | — |
| [12](12-interview-survival-kit.md) | 面试存活包 — 5/15/45 分钟讲法 + 常见尖刻问题 + 准备好的答案 | — |

## 怎么用每一章

每章都按同样的结构写,你可以养成读章节的肌肉记忆:

1. **这章你会学到什么** — 一句话告诉你目标
2. **关联前后** — 跟上一章接哪里,通往下一章干嘛
3. **背景概念(30 秒补课)** — 这一章里会用到但前文没讲过的词
4. **这一阶段的目标** — 我们解决了什么问题
5. **设计决策怎么做的** — 不是"我用了 X",而是"我考虑过 X / Y / Z,选 X 是因为 …"
6. **代码逐行讲** — 打开 repo 文件对照,每段告诉你为什么这么写
7. **验证 — 怎么知道做对了** — invariant / test / verifier
8. **You will be able to say(interview script)** — 英文的 1 分钟和 3 分钟两个版本
9. **常见尖刻问题 + 准备好的答案** — 面试官最爱问的几个 trap

## 关于这份 walkthrough 本身

这是给你看的学习材料,**不要打包给面试官**——面试官读 README + 进 repo 看 code。这份 walkthrough 帮你 *把这些东西讲出来*。

如果时间紧,先读章 00、12,再补 02 / 04 / 09(三个最有 senior signal 的章节)。

Ready? 翻 [00-overview.md](00-overview.md)。
