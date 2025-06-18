# DataSurface Architectural Decisions

## Overview

This document explains the architectural decisions behind DataSurface's design and analyzes how it differs from traditional data catalog approaches. DataSurface emerged from operating data platforms at enterprise scale (8 x 32-core HBase clusters, 2000-node Hadoop environments, millions of jobs daily) where conventional approaches encountered scaling and operational challenges.

**Core Principle:** DataSurface recognizes that metadata and operational data have fundamentally different characteristics and should use different storage strategies optimized for their specific access patterns and requirements.

## Understanding the Metadata vs Operational Data Distinction

### The Fundamental Difference

**Metadata Changes:**

- **Changed by:** People (data engineers, analysts, governance teams)
- **Frequency:** Low (10-100 changes per day across enterprise)
- **Scrutiny:** Highly reviewed (approvals, compliance, impact analysis)
- **Pattern:** Individual transactions with full context and history
- **Requirements:** Audit trails, access control, immutability, branching/merging

**Operational Data Changes:**

- **Changed by:** Software (pipelines, ETL jobs, streaming processes)
- **Frequency:** High (millions of writes per day)
- **Scrutiny:** Automated monitoring, not individual review
- **Pattern:** Bulk operations, ACID transactions, concurrent writes
- **Requirements:** Performance, consistency, backup/recovery

### Why This Matters

Traditional data catalogs treat both types identically, using the same database for human-driven metadata changes and software-driven operational updates. This approach optimizes for neither use case effectively.

## Traditional Data Catalog Challenges

### Scaling Limitations at Enterprise Level

Traditional catalogs face specific bottlenecks at large scale:

**Infrastructure Requirements:**

- Significant hardware dedicated to metadata serving (observed: 50 JVMs on 32-core servers)
- Database connection pooling and caching complexity
- Query optimization challenges for metadata-heavy workloads
- Linear relationship between job volume and metadata infrastructure needs

**Workflow System Limitations:**

- Cloud platforms (AWS Glue, Azure Data Factory) struggle beyond ~100 nodes in job graphs
- Metadata lookups become bottlenecks for batch processing workloads
- Complex dependency graphs require specialized handling

### Compliance Implementation Complexity

Enterprise compliance requirements typically force vendors to implement:

- Custom audit logging systems
- Proprietary approval workflows
- Role-based access control systems
- Validation and change tracking mechanisms
- Enterprise authentication integration

These implementations vary in quality and often require significant customization for enterprise requirements.

### Technology Lock-in Concerns

Traditional catalogs often couple metadata management to specific technology choices, creating migration challenges as data platform technologies evolve (typically every 3-4 years).

## DataSurface's Approach

### Git-as-Database for Metadata

DataSurface uses Git repositories to store metadata, treating the entire data model as a versioned, distributed artifact.

**Architecture Pattern:**

```text
┌─────────────────┐    git pull     ┌──────────────────┐
│ Git Repository  │ ─────────────▶ │ Python Server 1  │ ─▶ REST API
│ (metadata)      │                │ (model in RAM)   │
└─────────────────┘                └──────────────────┘
       ▲
       │ git push                    ┌──────────────────┐
┌─────────────────┐                 │ Python Server 2  │ ─▶ REST API
│ Background      │                 │ (model in RAM)   │
│ Validator       │                 └──────────────────┘
└─────────────────┘
```

**Advantages for Metadata Workloads:**

- Eliminates network latency for metadata queries (local RAM access)
- Atomic commits ensure model consistency
- Native versioning and branching capabilities
- Distributed architecture with no single points of failure
- Linear scaling through additional servers

**Trade-offs:**

- **Model size limitations** - Git repositories become unwieldy beyond ~10k-100k objects (datasets, schemas, policies)
- **Query constraints** - No SQL-style queries, requires in-memory traversal
- **Bootstrap overhead** - Initial model loading scales with model complexity
- **Tooling gaps** - Limited ecosystem compared to traditional databases

### Compliance Through Standard Tools

DataSurface leverages Git's established enterprise security model:

**Audit Capabilities:**

```bash
# Change tracking
git blame schemas/customer_data.py
git log --follow policies/data_retention.py

# Authorization enforcement
# .github/CODEOWNERS
/governance/eu_zone/     @eu-data-governance-team
/datastores/financial/   @finance-data-stewards @audit-team
```

**Process Integration:**

- Branch protection for required reviews
- Cryptographically signed commits
- GitHub Actions for automated validation
- Standard enterprise authentication (SAML/OIDC)

**Benefits:**

- Leverages 20+ years of Git security hardening
- Uses tools auditors already understand
- No custom compliance system development required
- Cryptographically verifiable audit trails

**Limitations:**

- Teams must adopt Git-based workflows for metadata changes
- May be overkill for smaller organizations
- Requires Git expertise beyond basic usage

### Git Scaling Characteristics

While Git-as-database introduces scaling constraints, these constraints align well with realistic enterprise metadata requirements:

**Scaling Sweet Spot:**

- **Target scale:** 10k datastores, 12 million datasets, 3k workspaces (40-50k datasets per container)
- **Git efficiency:** Repositories with 10k-100k tracked objects remain highly performant
- **Memory footprint:** Full model typically fits in 1-10GB RAM across server fleet
- **Network efficiency:** Git's delta compression minimizes transfer overhead

**Scaling Limitations:**

- **Repository size:** Git performance degrades significantly beyond 100k+ objects
- **Clone time:** Initial repository clones grow linearly with object count
- **Merge complexity:** Large repositories increase merge conflict resolution overhead
- **File system limits:** Individual file count and directory depth constraints

**Mitigation Strategies:**

- **Model partitioning:** Split extremely large ecosystems across multiple repositories
- **Shallow clones:** Use `--depth` flags for servers that don't need full history
- **Garbage collection:** Regular Git maintenance to optimize repository size
- **Caching layers:** CDN-style caching for frequently accessed model fragments

**When Git Scaling Becomes Limiting:**

- Metadata objects exceeding 1M+ entities
- Organizations with >100k individual datasets requiring separate tracking
- Real-time metadata update requirements (sub-second consistency needs)
- Environments where Git infrastructure expertise is unavailable

For most enterprise data platforms, Git scaling limits are reached only at exceptional scale, well beyond typical organizational data catalog requirements.

### Technology Abstraction Layer

DataSurface separates logical data requirements from physical platform implementations through intention graphs.

**Platform-Agnostic Model:**

```text
User Requirements → Intention Graph → Platform-Specific Implementation
```

This allows migration between technology stacks (Spark→BigQuery, Hadoop→Kubernetes) without requiring user workflow changes.

**Benefits:**

- Reduced technology lock-in
- Ability to run multiple platforms simultaneously
- Simplified technology migration paths

**Considerations:**

- Abstraction layer complexity
- Platform-specific optimizations may be limited
- Requires ongoing investment in platform adapters

## Implementation Details

### Python DSL Choice

Python was selected for the domain-specific language despite scale considerations:

**Advantages:**

- Clean, readable syntax for data structure definition
- Strong typing support through modern tooling (pylance/mypy)
- Natural integration with data ecosystem tools
- Rapid iteration during design phases

**Trade-offs:**

- Performance characteristics at extreme scale
- Runtime validation overhead
- Memory usage for large models

### Architectural Separation

**Metadata Storage (Git + RAM):**

- Schema definitions, policies, governance rules
- Low write frequency, high read volume
- Version control and audit requirements
- Served from local memory for performance

**Operational State (Traditional Database):**

- Job executions, pipeline state, metrics
- High write frequency, complex queries
- ACID transaction requirements
- Platform-dependent implementation (PostgreSQL→HBase→BigQuery)

This separation optimizes each storage system for its specific access patterns and requirements.

## Applicability Analysis

### Where This Approach Works Well

**Large Enterprises:**

- High metadata query volumes relative to updates
- Strong compliance and audit requirements
- Existing Git-based development processes
- Resources for custom tooling development

**Regulated Industries:**

- Financial services (SOX compliance)
- Healthcare (HIPAA audit trails)
- Government (security audit requirements)

### Where Traditional Approaches May Be Better

**Small Organizations:**

- Git workflow overhead may exceed benefits
- Simpler catalog tools may be more appropriate
- Limited compliance requirements

**High-Frequency Metadata Updates:**

- Real-time schema discovery scenarios
- Dynamic metadata generation from automated processes
- Complex metadata relationships requiring SQL-style queries

**Resource-Constrained Environments:**

- Limited development resources for custom tooling
- Preference for vendor-supported solutions
- Existing investments in traditional catalog tools

## Limitations and Considerations

**Technical Constraints:**

- Git repository size limitations beyond 100k+ metadata objects (typically 10x+ larger than most enterprise catalogs)
- Memory requirements for full model loading (1-10GB RAM per server, acceptable for most deployments)
- Limited query capabilities compared to SQL databases (compensated by in-memory performance)
- Custom tooling development and maintenance overhead

**Organizational Requirements:**

- Team training on Git workflows for metadata management
- Process changes from direct database editing to PR-based workflows
- Integration work for existing tools expecting database connections

**Operational Complexity:**

- Deployment and management of Python server clusters
- Monitoring and alerting for Git-based infrastructure
- Backup and disaster recovery procedures for Git repositories

## Conclusion

DataSurface represents a specialized architectural approach optimized for enterprise-scale metadata management. The core insight—that metadata and operational data have different characteristics requiring different storage strategies—addresses specific challenges encountered at large scale.

The approach trades traditional database flexibility for improved scaling characteristics and compliance integration. Git-as-database scaling constraints (affecting repositories beyond 100k+ objects) are rarely reached in practice, as most enterprise data catalogs manage 10k-100k metadata entities—well within Git's performance sweet spot.

**Optimal Use Cases:**

- Large enterprises with 1k-50k datastores and proportional metadata complexity
- Organizations requiring strong audit trails and compliance workflows
- Environments with high metadata read-to-write ratios (typical of most data platforms)
- Teams already using Git-based development processes

**Less Suitable For:**

- Organizations managing >100k distinct metadata objects requiring real-time updates
- Environments lacking Git infrastructure expertise
- Use cases requiring complex metadata queries not easily expressed through code traversal

The architectural decisions reflect practical solutions to real operational challenges rather than theoretical improvements, making DataSurface most valuable in contexts similar to those that drove its development. For the vast majority of enterprise data platforms, Git scaling constraints are not limiting factors, while the benefits of versioned, distributed, auditable metadata management provide substantial operational advantages.
