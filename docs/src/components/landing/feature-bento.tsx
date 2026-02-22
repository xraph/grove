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
  colSpan?: number;
}

const features: FeatureCard[] = [
  {
    title: "Native Query Syntax",
    description:
      "Each driver exposes its database's native idioms. PostgreSQL queries use $1 placeholders, MySQL uses backticks, MongoDB uses native BSON. No unified DSL — maximum performance and syntax fidelity.",
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
    code: `// PostgreSQL — native syntax
db.NewSelect(&users).
    Where("email ILIKE $1", "%@example.com").
    Where("metadata->>'tier' = $2", "premium").
    DistinctOn("email").
    Limit(50).
    Scan(ctx)`,
    filename: "pg_query.go",
  },
  {
    title: "Dual Tag System",
    description:
      'Define models with grove:"..." tags or use existing bun:"..." tags as fallback. When both are present, grove wins. Zero-cost migration from bun — existing models work unchanged.',
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
        <path d="M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z" />
        <line x1="7" y1="7" x2="7.01" y2="7" />
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
    title: "Zero-Reflection Queries",
    description:
      "Reflection happens once at model registration. The hot path — query building, execution, and scanning — uses cached field offsets and pooled buffers. Target: \u22645% overhead vs raw driver.",
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
        <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
      </svg>
    ),
    code: `// Reflect once at startup
db.RegisterModel((*User)(nil))

// Hot path: zero reflection, pooled buffers
var users []User
err := pgdb.NewSelect(&users).
    Where("active = $1", true).
    Scan(ctx)`,
    filename: "performance.go",
  },
  {
    title: "Modular Migrations",
    description:
      "Migrations are Go code, not SQL files. Any Go module can register migrations with dependency-aware ordering. Forge extensions ship their own migrations that compose automatically.",
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
    title: "Privacy Hooks",
    description:
      "Hook interfaces run before every query and mutation. Inject tenant isolation filters, redact PII fields, or log to audit trails — without implementing authorization logic in the ORM.",
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
            {Clause: "tenant_id = $1", Args: []any{tenantID}},
        },
    }, nil
}`,
    filename: "hooks.go",
  },
  {
    title: "Streaming & CDC",
    description:
      "Stream[T] is a lazy, pull-based generic iterator for database results. Supports composable pipeline transforms (Map, Filter, Chunk, Reduce) and Go 1.23+ range-over-func. ChangeStream[T] adds CDC support.",
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
    code: `// Lazy stream with per-row hooks
s, _ := pgdb.NewSelect(&User{}).
    Where("active = $1", true).
    Stream(ctx)
defer s.Close()

// Pipeline transforms
active := stream.Filter(s, func(u User) bool {
    return u.Email != ""
})
names := stream.Map(active, func(u User) (string, error) {
    return u.Name, nil
})`,
    filename: "streaming.go",
  },
  {
    title: "Multi-Driver Support",
    description:
      "One set of models works across PostgreSQL, MySQL, SQLite, and MongoDB. Each driver generates native syntax for its database while sharing the model registry, migration system, and hook engine.",
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
    code: `// Each driver opens its own connection
pgdrv := pgdriver.New()
pgdrv.Open(ctx, pgDSN)
pgDB, _ := grove.Open(pgdrv)

mydrv := mysqldriver.New()
mydrv.Open(ctx, myDSN)
myDB, _ := grove.Open(mydrv)

// Each generates native syntax
pgdriver.Unwrap(pgDB).NewSelect(&users).Where("email ILIKE $1", p)
mysqldriver.Unwrap(myDB).NewSelect(&users).Where("email LIKE ?", p)`,
    filename: "drivers.go",
    colSpan: 2,
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

export function FeatureBento() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Features"
          title="Everything you need for data access"
          description="Grove handles the hard parts — tag parsing, query building, result scanning, migrations, and privacy hooks — so you can focus on your application."
        />

        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: "-50px" }}
          className="mt-14 grid grid-cols-1 md:grid-cols-2 gap-4"
        >
          {features.map((feature) => (
            <motion.div
              key={feature.title}
              variants={itemVariants}
              className={cn(
                "group relative rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm p-6 hover:border-blue-500/20 hover:bg-fd-card/80 transition-all duration-300",
                feature.colSpan === 2 && "md:col-span-2",
              )}
            >
              {/* Header */}
              <div className="flex items-start gap-3 mb-4">
                <div className="flex items-center justify-center size-9 rounded-lg bg-blue-500/10 text-blue-600 dark:text-blue-400 shrink-0">
                  {feature.icon}
                </div>
                <div>
                  <h3 className="text-sm font-semibold text-fd-foreground">
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
          ))}
        </motion.div>
      </div>
    </section>
  );
}
