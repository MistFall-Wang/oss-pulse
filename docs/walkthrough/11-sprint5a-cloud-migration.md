# Chapter 11 — Sprint 5a:Cloud 迁移(真上 AWS)

## 这章你会学到什么

把本地 Bronze 真上到 AWS S3,且用 Terraform 把基础设施版本化。读完你能解释:**Terraform 的工作流(init/plan/apply),IAM/KMS 权限不够时怎么"降级 apply",aws s3 sync 怎么保留 Delta 的 `_delta_log` ,以及"本地 Spark 通过 hadoop-aws 读 S3 上的 Delta"为什么是个 senior signal 的 portability proof point**。

## 关联前后

- **上一章** ([Ch 10](10-sprint6-streaming-mvp.md)) 完成 streaming MVP
- **下一章** ([Ch 12](12-interview-survival-kit.md)) 是面试存活包,讲怎么把这一切串起来

## 背景概念(30 秒补课)

- **IaC(Infrastructure as Code)**:基础设施用代码描述。Terraform 是 IaC 工具的代表,你写 `.tf` 文件描述"要 3 个 S3 bucket、1 个 KMS key",Terraform 帮你 create/update/destroy。
- **Terraform 工作流**:`init` (下载 provider)→ `plan` (算变更,不动云)→ `apply` (真的动云)→ `destroy` (拆掉)。
- **`.tfstate`**:Terraform 自己的"我管理过哪些资源"账本。绝不能丢,绝不能跟资源真实状态脱节。
- **AWS S3**:对象存储,几乎不限容量,$0.023/GB-month。我们把 Bronze 数据放这里。
- **`s3a://` URI**:hadoop-aws connector 用的 S3 URI 协议,等价于 `s3://` 但更现代。
- **`hadoop-aws`**:Apache Hadoop 的 S3 文件系统实现。Spark 加这个 jar 就能读 S3。
- **IAM(Identity and Access Management)**:AWS 的权限管理。每个用户 / 角色 / 服务能做什么操作,通过 IAM policy 控制。
- **least privilege**:只给某个 user / role 完成任务必须的权限,不多给。我们的 `de-portfolio-cli` 用户就是 least-privilege bootstrap 用户。

## 这一阶段的目标

1. 写 Terraform `.tf` 文件描述云资源:3 个 S3 bucket(bronze/silver/gold)、KMS key(加密)、IAM role(给未来的 Databricks 用)、生命周期策略
2. **真 apply 一次**,在 AWS 上创建资源
3. 把本地 Bronze 数据上传到 S3 bucket
4. **用本地 Spark 通过 hadoop-aws 读 S3 上的 Bronze**,验证 ADR-0002 invariant 在云端 round-trip 后仍成立
5. 写一份完整的"我自己 apply 怎么做"walkthrough doc

## 设计决策怎么做的

### 决策 1:几个 S3 bucket(1 还是 3)

候选:

| 选项 | 优 | 劣 |
|------|----|----|
| 1 个 bucket,3 个 prefix(`/bronze/`, `/silver/`, `/gold/`) | 命名简洁,管理 IAM 简单 | 不能给每层不同生命周期 |
| **3 个 bucket**(每层一个) | 各层独立生命周期 / 加密 / 访问控制 | bucket 名要全局唯一,3 倍管理 |

选 3 个。理由:Bronze 老数据可以归档到 IA(便宜),Silver 是衍生的(versioning 无意义),Gold 是 serving 层(versioning 防 accidental delete)。不同行为只能用不同 bucket 表达。

### 决策 2:KMS-encrypted 还是 SSE-S3

| 选项 | 优 | 劣 |
|------|----|----|
| **customer-managed KMS key** | 合规等级,你控制 key rotation,可以审计每次解密 | $1/月 per key + 需要 `kms:CreateKey` 权限 |
| AWS-managed SSE-S3(AES256) | 0 成本,AWS 自动启用(2023+ 新 bucket) | 不可控制 key,合规上稍弱 |

**原设计**:customer-managed KMS。**实际部署**:遇到 IAM 权限不够,降级为 SSE-S3。

这是个**真实场景的 portfolio 故事点**——大多数 portfolio 写"我用了 KMS",但没真 apply 过,不知道 `de-portfolio-cli` 没 `kms:CreateKey` 权限怎么办。我们遇到了,降级了,写下了 step 9.0 fallback。

### 决策 3:遇到权限不够怎么办

候选:

| 选项 | 优 | 劣 |
|------|----|----|
| (a) 给 `de-portfolio-cli` 加 `iam:CreateRole` + `kms:CreateKey` | 完整 apply | 违反 least-privilege,生产环境真 reviewer 会问 |
| (b) **降级 apply**:删 iam.tf,删 KMS 资源,让 SSE-S3 自动启用 | 不打破 least-privilege | 项目缺少 KMS/IAM 落地证据 |
| (c) 创建新 IAM user 给足权限 | 完整 apply | 维护两个 user 麻烦 |

选 (b)。**关键 reasoning**:

- KMS 是"nice-to-have",SSE-S3 也是加密
- IAM role for Databricks 真等 Databricks 上线再加
- portfolio 想展示"我懂 Terraform"——3 个 bucket + 生命周期 + 加密就足够
- **写一份 step 9.0 fallback runbook** 比硬 apply 更有 senior signal——它说明"我遇到现实问题,有应对方案,文档化了"

## 代码逐行讲

### `terraform/versions.tf`

```hcl
terraform {
  required_version = ">= 1.6.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.70"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project    = "oss-pulse"
      ManagedBy  = "terraform"
      Repository = "github.com/MistFall-Wang/oss-pulse"
    }
  }
}
```

要点:

- `required_version` 锁 Terraform 版本下限,防止用旧版 Terraform 误操作
- `~> 5.70` 是 Terraform 的 pessimistic constraint —— **`>= 5.70, < 6.0`**。允许 patch / minor 升级,major 升级要 explicit
- `default_tags`:**每个资源自动加 3 个 tag**。这是 cost attribution 和 ownership 跟踪的基础。Senior 必做

### `terraform/variables.tf`

```hcl
variable "aws_region" {
  description = "AWS region for all OSS Pulse resources."
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment label appended to bucket names so dev / prod can coexist."
  type        = string
  default     = "dev"
  validation {
    condition     = contains(["dev", "stage", "prod"], var.environment)
    error_message = "environment must be one of: dev, stage, prod."
  }
}

variable "bucket_suffix" {
  description = "Random suffix appended to bucket names to make them globally unique. Generate once: openssl rand -hex 4."
  type        = string
}
```

每个 variable 都有 description + 类型。`environment` 加 validation 防止 typo。`bucket_suffix` 必填没 default——逼用户每次明确指定。

### `terraform/s3.tf`(主)

```hcl
locals {
  bronze_bucket = "oss-pulse-bronze-${var.environment}-${var.bucket_suffix}"
  silver_bucket = "oss-pulse-silver-${var.environment}-${var.bucket_suffix}"
  gold_bucket   = "oss-pulse-gold-${var.environment}-${var.bucket_suffix}"
}

resource "aws_s3_bucket" "bronze" {
  bucket = local.bronze_bucket
}
resource "aws_s3_bucket" "silver" { bucket = local.silver_bucket }
resource "aws_s3_bucket" "gold"   { bucket = local.gold_bucket }

# Block all public access on every bucket.
resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
# (3 个 bucket 每个一份 public_access_block)

# Versioning: Bronze + Gold 开,Silver 不开
resource "aws_s3_bucket_versioning" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  versioning_configuration { status = "Enabled" }
}
resource "aws_s3_bucket_versioning" "gold" {
  bucket = aws_s3_bucket.gold.id
  versioning_configuration { status = "Enabled" }
}

# Lifecycle: Bronze 60 天后转 Standard-IA
resource "aws_s3_bucket_lifecycle_configuration" "bronze" {
  bucket = aws_s3_bucket.bronze.id
  rule {
    id     = "tier-cold-bronze-to-ia"
    status = "Enabled"
    filter { prefix = "events/" }
    transition {
      days          = 60
      storage_class = "STANDARD_IA"
    }
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}
```

要点:

- `locals` 算出 bucket 名:`oss-pulse-bronze-dev-9f3eb8a5`。`9f3eb8a5` 是随机 hex 后缀,保证全球唯一
- 每个 bucket 都加 `public_access_block`——4 个 flag 全开是 **数据 lakehouse 的强制安全配置**。reviewer 看这段,知道你不会把数据公开到 internet
- `versioning`: Bronze 是 source of truth,Gold 是 serving——这两个 accidental delete 最痛。Silver 是 derived,不开省 storage
- `lifecycle_configuration` for Bronze: 60 天后自动转 IA,**ADR-0007 storage 成本估算的延伸**——old data 不删但便宜。`noncurrent_version_expiration = 7d` 是 versioning 配套:老版本(versioning 保留的旧 object)只保留 7 天

### `terraform/iam.tf`(被 disabled 的部分)

最初是这样写的:

```hcl
data "aws_iam_policy_document" "databricks_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "AWS"
      identifiers = ["arn:aws:iam::${var.databricks_aws_account_id}:root"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "databricks_lakehouse" {
  name               = "oss-pulse-${var.environment}-databricks-lakehouse"
  assume_role_policy = data.aws_iam_policy_document.databricks_assume_role.json
}
```

含义:创建一个 IAM role,允许 Databricks 的官方 AWS 账号(`414351767826`)assume 它,这是 cross-account access 的标准模式。

但实际 apply 时报错:

```
AccessDenied: User: arn:aws:iam::703705584598:user/de-portfolio-cli
is not authorized to perform: iam:CreateRole
```

`de-portfolio-cli` 没 `iam:CreateRole`。我们的应对:**`git mv iam.tf iam.tf.disabled`**——文件还在(reviewer 能看到设计),但 Terraform 不会再处理它(因为 `.disabled` 后缀不是 `.tf`)。

这就是"我没硬 apply 通过,而是 graceful degrade"的 portfolio 故事。

### `terraform/terraform.tfvars`(gitignored)

```hcl
aws_region    = "us-east-1"
environment   = "dev"
bucket_suffix = "9f3eb8a5"
```

这文件**不上 git**(在 `.gitignore` 里)。它包含 user-specific 信息(bucket suffix),不应该跟代码混。每个 dev 自己 `cp terraform.tfvars.example terraform.tfvars` 然后改。

### 实际 apply 跑出来

```bash
# 1. terraform init - 下载 AWS provider
terraform init
# Terraform has been successfully initialized!

# 2. terraform plan - 算变更
terraform plan -out=tfplan
# Plan: 16 to add, 0 to change, 0 to destroy.

# 3. terraform apply - 真创建
terraform apply tfplan
# Partial success: 3 S3 buckets + versioning + lifecycle created.
# 2 errors: iam:CreateRole denied, kms:TagResource denied.

# 4. 我们的 fallback: git mv iam.tf iam.tf.disabled
# + 删 KMS resource from s3.tf
# + terraform plan (no changes)
# + done

# 5. 同步本地 Bronze 到 S3
BRONZE=oss-pulse-bronze-dev-9f3eb8a5
aws s3 sync data/bronze/events s3://$BRONZE/events/ --exact-timestamps
# 28 objects, 444 MB uploaded
```

`--exact-timestamps` 这一行不只是性能优化——它保证 `_delta_log/` 里的 commit JSON 时间戳跟本地一致。Delta 用 timestamp 判断 commit 顺序,**如果 sync 时改了时间戳,Delta 表读不出来或读出错误的 history**。

### `spark/jobs/s3_smoke_test.py`(关键 — 验证 S3 上的 Delta 真可读)

```python
import boto3
from pyspark.sql import SparkSession


def aws_creds() -> tuple[str, str, str | None]:
    """Pull effective AWS credentials from the same chain aws CLI uses."""
    session = boto3.Session()
    creds = session.get_credentials()
    if creds is None:
        raise SystemExit("[s3_smoke_test] no AWS credentials found.")
    frozen = creds.get_frozen_credentials()
    return frozen.access_key, frozen.secret_key, frozen.token


def build_spark(bucket: str) -> SparkSession:
    access_key, secret_key, session_token = aws_creds()

    # All jars must be on classpath BEFORE getOrCreate().
    if "hadoop-aws" not in os.environ.get("PYSPARK_SUBMIT_ARGS", ""):
        os.environ["PYSPARK_SUBMIT_ARGS"] = (
            "--driver-memory 4g "
            "--packages io.delta:delta-spark_2.12:3.2.1,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "pyspark-shell"
        )

    builder = (
        SparkSession.builder.appName("s3_smoke_test")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    )
    if session_token:
        builder = builder.config("spark.hadoop.fs.s3a.session.token", session_token)
        # ... use TemporaryAWSCredentialsProvider ...

    return builder.getOrCreate()


def main():
    args = parser.parse_args()
    path = f"s3a://{args.bucket}/{args.prefix}"

    spark = build_spark(args.bucket)
    df = spark.read.format("delta").load(path)
    
    total = df.count()
    distinct = df.select("id").distinct().count()
    invariant = total == distinct

    print(f"total rows: {total:,}")
    print(f"distinct ids: {distinct:,}")
    print(f"invariant: {invariant}")

    df.groupBy("ingest_hour").count().orderBy("ingest_hour").show(20)

    if not invariant:
        sys.exit(1)
```

要点:

- **`aws_creds()`** 用 boto3 的 credential chain 拉 AWS 凭证(`~/.aws/credentials` 或 env vars)。这一步让"AWS CLI 能跑就 Spark 也能跑",reviewer 不需要额外配置
- **PYSPARK_SUBMIT_ARGS 同时 include 3 个包**:Delta + hadoop-aws + aws-java-sdk-bundle。少一个 Spark 就读不了 S3
- `spark.hadoop.fs.s3a.access.key/.secret.key` 把 boto3 拿到的 creds 推给 Spark
- `s3.amazonaws.com` 是 default endpoint;**如果用 MinIO / R2 / 其他 S3-compatible store,只改这一行**——portability proof

### 实际跑出来的 smoke test

```
[s3_smoke_test] reading s3a://oss-pulse-bronze-dev-9f3eb8a5/events ...

========== S3 Bronze smoke test ==========
path:                 s3a://oss-pulse-bronze-dev-9f3eb8a5/events
total rows:           613,876
distinct ids:         613,876
invariant (ADR-0002): total == distinct → True

[breakdown] rows per ingest_hour:
+-------------+------+
|ingest_hour  |count |
+-------------+------+
|2015-01-15-12|21062 |
|2018-01-15-12|63463 |
|2025-01-15-12|270553|
|2025-01-15-13|258798|
+-------------+------+

[s3_smoke_test] PASS — cloud Bronze matches local Bronze.
```

**这就是 Sprint 5a 的真证据**:

1. Bronze 在 S3 上真的活着
2. 总行数 = 613,876,跟本地一致
3. ADR-0002 invariant(idempotency)在云端 round-trip 后仍成立
4. 4 个 ingest_hour 分区行数与本地完全相同

这是 portfolio 项目里 **极少数能写"我真在 AWS 上跑过"** 的环节。

## 验证 — 这阶段怎么知道做对了

```bash
# 1. terraform output 看 bucket 名
terraform output bronze_bucket
# "oss-pulse-bronze-dev-9f3eb8a5"

# 2. aws CLI 验证 bucket 真存在
aws s3 ls s3://oss-pulse-bronze-dev-9f3eb8a5/events/ --recursive | wc -l
# 28

# 3. smoke test invariant 通过
uv run python -m spark.jobs.s3_smoke_test --bucket oss-pulse-bronze-dev-9f3eb8a5
# PASS
```

这三个 evidence 加起来 = Sprint 5a done。

## Tear-down(完整故事)

```bash
# 清空 buckets (terraform destroy 不删非空 bucket)
for B in $BRONZE $SILVER $GOLD; do
  aws s3 rm "s3://$B" --recursive
  # versioned buckets 需要 delete versions
  aws s3api delete-objects --bucket "$B" \
    --delete "$(aws s3api list-object-versions ...)"
done

# Destroy
cd terraform && terraform destroy
```

费用回到 $0。这个 step 在 walkthrough doc 也写了——senior 知道**真正 portfolio 价值在"我能 set up 也能 tear down"**,不要一直让 AWS 烧钱。

## 代码 review 笔记

`s3_smoke_test.py` 第 90 行附近的 if 分支:

```python
if session_token:
    builder = builder.config("spark.hadoop.fs.s3a.session.token", session_token)
    builder = builder.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
```

这一段是为了支持 AWS STS temporary credentials(SSO / role assumption)。**之前 aws_creds 拉的可能是 frozen token**。如果用户用 `aws sso login` 之类的方式认证,这段是必须的。

普通 IAM user(`de-portfolio-cli`)没 session_token,这段 skip。但代码留着,以后 user 切换 SSO 不用改一行。**senior code 的特征**:对未来场景留好 hook,不是为了当下功能。

## You will be able to say

### 3-minute version (English)

> "Sprint 5a is the cloud migration. Terraform-managed AWS, real
> apply, real S3 Bronze. Five files:
>
> versions.tf pins Terraform >= 1.6 and the AWS provider. Default
> tags get applied to every resource — Project, ManagedBy, Repository
> — for cost attribution.
>
> s3.tf defines three buckets, one per medallion layer. Each has
> a public-access block with all four flags true — data lakehouse
> data should never be internet-readable. Bronze and Gold have
> versioning; Silver doesn't because it's derived. Bronze has a
> 60-day lifecycle to Standard-IA — the storage cost math from
> ADR-0007.
>
> iam.tf has the Databricks cross-account role design — but it's
> been renamed iam.tf.disabled. My bootstrap IAM user is
> least-privilege; it can do S3 but not iam:CreateRole. Real
> production would either grant that permission to a separate
> bootstrap role, or apply iam.tf manually from a higher-privileged
> session. My walkthrough doc has both paths.
>
> Same story for KMS — original design had a customer-managed key
> for SSE-KMS. de-portfolio-cli lacks kms:CreateKey. Fallback:
> drop KMS, let SSE-S3 (AES256, AWS-managed key) apply by default
> on new buckets. Still encrypted at rest, just AWS-managed.
>
> The apply ran: 16 plan, 14 created, 2 errors caught by the
> least-privilege wall. Buckets came up clean. I aws-s3-synced
> data/bronze/events — 28 objects, 444 MB, including the
> `_delta_log` directory which is what makes the Delta table
> readable.
>
> Then I wrote `spark/jobs/s3_smoke_test.py`. It uses boto3's
> credential chain to find AWS creds (same way as the AWS CLI),
> pushes them into Spark's hadoop-aws config, and reads Bronze
> from s3a://. The output: 613,876 rows, 613,876 distinct ids,
> ADR-0002 invariant holds, per-ingest_hour breakdown matches
> local exactly. So I've now proven the cloud Bronze is real and
> the same idempotency story works through the S3 round-trip."

## 常见尖刻问题 + 准备好的答案

**Q: "你 KMS 没做完,怎么 ship?"**

> "KMS was a design choice I'd make for production. With my
> bootstrap user's IAM scope, I had two options: grant
> kms:CreateKey or fall back to SSE-S3. I fell back — SSE-S3 is
> still encrypted at rest, just with an AWS-managed key. The
> step-9.0 fallback runbook documents both the choice and the
> re-enable path. For a portfolio, showing the trade-off is more
> honest than fabricating the apply."

**Q: "S3 bucket 名是 `oss-pulse-bronze-dev-9f3eb8a5`,为什么有那个 9f3eb8a5?"**

> "Bucket names are globally unique in S3. The hex suffix
> generated by `openssl rand -hex 4` prevents collisions, both
> across my own re-applies and with anyone else. The variable's
> validation forces fresh suffixes per environment so dev/stage/prod
> never share."

**Q: "你 Spark 跑在本地,但读 S3 上的 Delta,这算 'on AWS' 吗?"**

> "It proves the data layer works on AWS independently of where
> compute runs. The next migration step is moving compute to
> Databricks Free Edition — that's documented in step 7 of the
> cloud-apply walkthrough but gated on a Databricks workspace
> sign-up I haven't done yet. The smoke test shows the contract
> the Databricks-compute layer would consume; until Databricks is
> wired, local Spark with hadoop-aws is the closest substitute and
> proves the contract works. It's not 'pretend cloud' — it's a
> portability proof point."

**Q: "你的 terraform.tfvars 没进 git,怎么 reproduce?"**

> "By design — tfvars contains the bucket suffix which is
> environment-specific. terraform.tfvars.example is checked in
> with placeholders. The walkthrough doc tells reviewers to copy,
> generate a fresh suffix with openssl, and apply. Every dev or
> reviewer gets their own non-colliding bucket names."

---

下一章 →  [12-interview-survival-kit.md](12-interview-survival-kit.md)
