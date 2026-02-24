"use client";

import { motion } from "framer-motion";
import { cn } from "@/lib/cn";
import { CodeBlock } from "./code-block";
import { SectionHeader } from "./section-header";

interface FeatureCard {
  title: string;
  description: string;
  icon: React.ReactNode;
  code: string;
  filename: string;
}

// ─── Tier 1: Hero capability cards ──────────────────────────
const heroFeatures: FeatureCard[] = [
  {
    title: "Polyglot ORM",
    description:
      "Native query syntax per database with dual grove/bun tag system. Zero-reflection hot path, cached field offsets, and pooled buffers for near-raw performance.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <ellipse cx="12" cy="5" rx="9" ry="3" />
        <path d="M3 5v7c0 1.66 4.03 3 9 3s9-1.34 9-3V5" />
        <path d="M3 12v7c0 1.66 4.03 3 9 3s9-1.34 9-3v-7" />
      </svg>
    ),
    code: `type User struct {
    grove.BaseModel \`grove:"table:users,alias:u"\`

    ID    int64  \`grove:"id,pk,autoincrement"\`
    Name  string \`grove:"name,notnull"\`
    Email string \`grove:"email,notnull,unique"\`
    SSN   string \`grove:"ssn,privacy:pii"\`
}`,
    filename: "model.go",
  },
  {
    title: "Offline-First CRDT",
    description:
      "LWW-Register, PN-Counter, and OR-Set types with automatic conflict resolution. Distributed nodes modify data independently and converge without coordination.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M4 12h4M16 12h4M12 4v4M12 16v4" />
        <circle cx="12" cy="12" r="4" />
      </svg>
    ),
    code: `plugin := crdt.NewPlugin(crdt.PluginConfig{
    NodeID:    "node-1",
    Tombstone: true,
})
plugin.Register("documents", crdt.LWWRegister)
plugin.Register("likes",     crdt.PNCounter)
plugin.Register("tags",      crdt.ORSet)`,
    filename: "crdt.go",
  },
  {
    title: "Universal KV Store",
    description:
      "5 backends — Redis, Memcached, DynamoDB, BoltDB, and Badger. Keyspaces for logical separation, composable middleware for logging, metrics, and circuit breakers.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M7 7h10M7 12h10M7 17h6" />
        <circle cx="4" cy="7" r="1.5" fill="currentColor" stroke="none" />
        <circle cx="4" cy="12" r="1.5" fill="currentColor" stroke="none" />
        <circle cx="4" cy="17" r="1.5" fill="currentColor" stroke="none" />
      </svg>
    ),
    code: `store := redis.New(redis.Config{Addr: ":6379"})
cache := kv.WithKeyspace(store, "cache:")
sessions := kv.WithKeyspace(store, "session:")

cache.Set(ctx, "user:123", data, 5*time.Minute)
val, _ := cache.Get(ctx, "user:123")`,
    filename: "kv.go",
  },
];

// ─── Tier 2: Secondary feature cards ────────────────────────
const secondaryFeatures: FeatureCard[] = [
  {
    title: "Multi-Database",
    description:
      "Named database connections with DBManager and vessel DI. Connect PostgreSQL, ClickHouse, and SQLite in a single app with per-DB hooks and migrations.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <rect x="3" y="3" width="7" height="7" rx="1" />
        <rect x="14" y="3" width="7" height="7" rx="1" />
        <rect x="3" y="14" width="7" height="7" rx="1" />
        <rect x="14" y="14" width="7" height="7" rx="1" />
      </svg>
    ),
    code: `ext := extension.New(
    extension.WithDatabase("primary", pgDrv),
    extension.WithDatabase("analytics", chDrv),
    extension.WithDefaultDatabase("primary"),
)`,
    filename: "multi_db.go",
  },
  {
    title: "Privacy Hooks",
    description:
      "Hook interfaces run before every query and mutation. Inject tenant isolation, redact PII fields, or log to audit trails without authorization logic in the ORM.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z" />
      </svg>
    ),
    code: `func (t *TenantIsolation) BeforeQuery(
    ctx context.Context, qc *hook.QueryContext,
) (*hook.HookResult, error) {
    return &hook.HookResult{
        Decision: hook.Modify,
        Filters: []hook.ExtraFilter{
            {Clause: "tenant_id = $1", Args: []any{tid}},
        },
    }, nil
}`,
    filename: "hooks.go",
  },
  {
    title: "Streaming & CDC",
    description:
      "Stream[T] is a lazy, pull-based generic iterator. Composable pipeline transforms (Map, Filter, Chunk, Reduce) and Go 1.23+ range-over-func. ChangeStream adds CDC.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M22 12h-4l-3 9L9 3l-3 9H2" />
      </svg>
    ),
    code: `s, _ := pgdb.NewSelect(&User{}).
    Where("active = $1", true).Stream(ctx)
defer s.Close()

active := stream.Filter(s, func(u User) bool {
    return u.Email != ""
})
names := stream.Map(active, func(u User) (string, error) {
    return u.Name, nil
})`,
    filename: "streaming.go",
  },
  {
    title: "Modular Migrations",
    description:
      "Go-code migrations with dependency-aware ordering. Forge extensions ship their own migrations that compose automatically across modules.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <rect x="2" y="2" width="20" height="6" rx="1" />
        <rect x="2" y="10" width="20" height="6" rx="1" />
        <path d="M6 18h12v3a1 1 0 01-1 1H7a1 1 0 01-1-1v-3z" />
      </svg>
    ),
    code: `var Migrations = migrate.NewGroup("forge.billing",
    migrate.DependsOn("core"),
)
Migrations.MustRegister(&migrate.Migration{
    Name: "create_invoices", Version: "20240201000000",
    Up: createInvoicesUp, Down: createInvoicesDown,
})`,
    filename: "migrations.go",
  },
  {
    title: "7 Database Drivers",
    description:
      "PostgreSQL, MySQL, SQLite, MongoDB, Turso, ClickHouse, and Elasticsearch. Each generates native syntax while sharing the model registry and hook engine.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
    ),
    code: `pgdrv := pgdriver.New()
pgdrv.Open(ctx, pgDSN)
pgDB, _ := grove.Open(pgdrv)

// Each generates native syntax
pg := pgdriver.Unwrap(pgDB)
pg.NewSelect(&users).Where("email ILIKE $1", p)`,
    filename: "drivers.go",
  },
  {
    title: "Observability",
    description:
      "Prometheus metrics, distributed tracing, and audit trail hooks. Track query latency, row counts, and log every mutation for compliance.",
    icon: (
      <svg
        className="size-5"
        viewBox="0 0 24 24"
        fill="none"
        stroke="currentColor"
        strokeWidth="1.5"
        strokeLinecap="round"
        strokeLinejoin="round"
        aria-hidden="true"
      >
        <path d="M18 20V10M12 20V4M6 20v-6" />
      </svg>
    ),
    code: `func (a *AuditHook) AfterMutation(
    ctx context.Context, model any,
    oldVal, newVal any,
) error {
    return a.chronicle.Log(ctx, chronicle.Entry{
        Action: "update", Table: "users",
        UserID: auth.UserID(ctx),
    })
}`,
    filename: "audit.go",
  },
];

const containerVariants = {
  hidden: {},
  visible: {
    transition: {
      staggerChildren: 0.08,
    },
  },
};

const itemVariants = {
  hidden: { opacity: 0, y: 20 },
  visible: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.5, ease: "easeOut" as const },
  },
};

// ─── Card Component ─────────────────────────────────────────
function FeatureCardView({
  feature,
  tier,
}: {
  feature: FeatureCard;
  tier: "hero" | "secondary";
}) {
  return (
    <motion.div
      variants={itemVariants}
      className={cn(
        "group relative rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm p-6 hover:border-blue-500/20 hover:bg-fd-card/80 transition-all duration-300",
        tier === "hero" && "ring-1 ring-blue-500/5",
      )}
    >
      {/* Header */}
      <div className="flex items-start gap-3 mb-4">
        <div
          className={cn(
            "flex items-center justify-center shrink-0 rounded-lg bg-blue-500/10 text-blue-600 dark:text-blue-400",
            tier === "hero" ? "size-10" : "size-9",
          )}
        >
          {feature.icon}
        </div>
        <div>
          <h3
            className={cn(
              "font-semibold text-fd-foreground",
              tier === "hero" ? "text-base" : "text-sm",
            )}
          >
            {feature.title}
          </h3>
          <p className="text-xs text-fd-muted-foreground mt-1 leading-relaxed">
            {feature.description}
          </p>
        </div>
      </div>

      {/* Code snippet */}
      <CodeBlock
        code={feature.code}
        filename={feature.filename}
        showLineNumbers={false}
        className="text-xs"
      />
    </motion.div>
  );
}

// ─── Feature Bento Section ──────────────────────────────────
export function FeatureBento() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Capabilities"
          title="Everything you need for data"
          description="ORM, CRDT, key-value store, streaming, hooks, and migrations — Grove is a complete data toolkit for Go applications."
        />

        {/* Tier 1: Hero capability cards */}
        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: "-50px" }}
          className="mt-14 grid grid-cols-1 md:grid-cols-3 gap-4"
        >
          {heroFeatures.map((feature) => (
            <FeatureCardView
              key={feature.title}
              feature={feature}
              tier="hero"
            />
          ))}
        </motion.div>

        {/* Tier 2: Secondary feature cards */}
        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: "-50px" }}
          className="mt-4 grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4"
        >
          {secondaryFeatures.map((feature) => (
            <FeatureCardView
              key={feature.title}
              feature={feature}
              tier="secondary"
            />
          ))}
        </motion.div>
      </div>
    </section>
  );
}
