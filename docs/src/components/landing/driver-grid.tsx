"use client";

import { motion } from "framer-motion";
import { cn } from "@/lib/cn";
import { CodeBlock } from "./code-block";
import { SectionHeader } from "./section-header";

// ─── Corner decorators ──────────────────────────────────────
function CardDecorator() {
  return (
    <>
      <span className="border-blue-500/40 absolute -left-px -top-px block size-2 border-l-2 border-t-2" />
      <span className="border-blue-500/40 absolute -right-px -top-px block size-2 border-r-2 border-t-2" />
      <span className="border-blue-500/40 absolute -bottom-px -left-px block size-2 border-b-2 border-l-2" />
      <span className="border-blue-500/40 absolute -bottom-px -right-px block size-2 border-b-2 border-r-2" />
    </>
  );
}

// ─── Data ───────────────────────────────────────────────────

interface FeaturedDriver {
  abbr: string;
  name: string;
  description: string;
  color: string;
  code: string;
  filename: string;
}

interface CompactDriver {
  abbr: string;
  name: string;
  description: string;
  color: string;
}

const featuredDrivers: FeaturedDriver[] = [
  {
    abbr: "PG",
    name: "PostgreSQL",
    description:
      "Native $1 placeholders, JSONB operators, DISTINCT ON, and ILIKE — full Postgres dialect.",
    color: "blue",
    code: `pg := pgdriver.Unwrap(db)

users := pg.NewSelect(&User{}).
    Where("email ILIKE $1", "%@acme.com").
    Where("metadata->>'role' = $2", "admin").
    OrderExpr("created_at DESC").
    Limit(20)`,
    filename: "postgres.go",
  },
  {
    abbr: "MY",
    name: "MySQL",
    description:
      "Backtick quoting, ? placeholders, ON DUPLICATE KEY upserts, and native JSON functions.",
    color: "orange",
    code: `my := mysqldriver.Unwrap(db)

_, err := my.NewInsert(&User{Name: name, Email: email}).
    On("DUPLICATE KEY UPDATE").
    Set("\`name\` = VALUES(\`name\`)").
    Exec(ctx)`,
    filename: "mysql.go",
  },
  {
    abbr: "MG",
    name: "MongoDB",
    description:
      "Native BSON queries, aggregation pipelines, change streams, and embedded document support.",
    color: "green",
    code: `mg := mongodriver.Unwrap(db)

results := mg.NewSelect(&Order{}).
    Where("status", "active").
    Where("total >", 100).
    Sort("-created_at").
    Limit(50)`,
    filename: "mongo.go",
  },
];

const compactDrivers: CompactDriver[] = [
  {
    abbr: "SQ",
    name: "SQLite",
    description: "Embedded storage with full SQL and WAL mode",
    color: "teal",
  },
  {
    abbr: "TU",
    name: "Turso",
    description: "Edge-replicated SQLite with libSQL",
    color: "purple",
  },
  {
    abbr: "CH",
    name: "ClickHouse",
    description: "Columnar analytics with batch inserts",
    color: "amber",
  },
  {
    abbr: "ES",
    name: "Elasticsearch",
    description: "Full-text search with JSON DSL",
    color: "indigo",
  },
];

const unifiedCode = `// One interface. Any driver. Zero changes.
pgdrv := pgdriver.New()
pgdrv.Open(ctx, "postgres://localhost/app")

chdrv := chdriver.New()
chdrv.Open(ctx, "clickhouse://localhost/analytics")

// Same grove.Open — native syntax per driver
pgDB, _ := grove.Open(pgdrv)
chDB, _ := grove.Open(chdrv)`;

// ─── Color maps ─────────────────────────────────────────────

const badgeColorMap: Record<string, string> = {
  blue: "bg-blue-500/10 text-blue-600 dark:text-blue-400 border-blue-500/20",
  orange:
    "bg-orange-500/10 text-orange-600 dark:text-orange-400 border-orange-500/20",
  green:
    "bg-green-500/10 text-green-600 dark:text-green-400 border-green-500/20",
  teal: "bg-teal-500/10 text-teal-600 dark:text-teal-400 border-teal-500/20",
  purple:
    "bg-purple-500/10 text-purple-600 dark:text-purple-400 border-purple-500/20",
  amber:
    "bg-amber-500/10 text-amber-600 dark:text-amber-400 border-amber-500/20",
  indigo:
    "bg-indigo-500/10 text-indigo-600 dark:text-indigo-400 border-indigo-500/20",
};

// ─── Animation ──────────────────────────────────────────────

const containerVariants = {
  hidden: {},
  visible: {
    transition: { staggerChildren: 0.08 },
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

// ─── Featured Driver Card ───────────────────────────────────

function FeaturedCard({ driver }: { driver: FeaturedDriver }) {
  return (
    <motion.div
      variants={itemVariants}
      className="group relative rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm overflow-hidden hover:border-blue-500/20 transition-all duration-300"
    >
      <CardDecorator />

      <div className="p-5 pb-3">
        <div className="flex items-center gap-3 mb-3">
          <div
            className={cn(
              "flex items-center justify-center size-9 rounded-lg font-mono text-xs font-bold border",
              badgeColorMap[driver.color],
            )}
          >
            {driver.abbr}
          </div>
          <div>
            <h3 className="text-sm font-semibold text-fd-foreground">
              {driver.name}
            </h3>
          </div>
        </div>
        <p className="text-xs text-fd-muted-foreground leading-relaxed">
          {driver.description}
        </p>
      </div>

      <div className="px-5 pb-5">
        <CodeBlock
          code={driver.code}
          filename={driver.filename}
          showLineNumbers={false}
          className="text-xs"
        />
      </div>
    </motion.div>
  );
}

// ─── Compact Driver Card ────────────────────────────────────

function CompactCard({ driver }: { driver: CompactDriver }) {
  return (
    <motion.div
      variants={itemVariants}
      className="group relative rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm p-4 hover:border-blue-500/20 transition-all duration-300"
    >
      <div className="flex items-center gap-3">
        <div
          className={cn(
            "flex items-center justify-center size-8 rounded-lg font-mono text-[10px] font-bold border shrink-0",
            badgeColorMap[driver.color],
          )}
        >
          {driver.abbr}
        </div>
        <div className="min-w-0">
          <h3 className="text-sm font-semibold text-fd-foreground">
            {driver.name}
          </h3>
          <p className="text-[11px] text-fd-muted-foreground leading-snug mt-0.5">
            {driver.description}
          </p>
        </div>
      </div>
    </motion.div>
  );
}

// ─── Unified API Card ───────────────────────────────────────

function UnifiedAPICard() {
  return (
    <motion.div
      variants={itemVariants}
      className="group relative rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm overflow-hidden lg:col-span-2 hover:border-blue-500/20 transition-all duration-300"
    >
      <CardDecorator />

      <div className="grid lg:grid-cols-[1fr_1.2fr] gap-0">
        {/* Left: text */}
        <div className="p-6 flex flex-col justify-center">
          <div className="flex items-center gap-2 mb-3">
            <div className="flex items-center justify-center size-9 rounded-lg bg-blue-500/10 text-blue-600 dark:text-blue-400">
              <svg
                className="size-4"
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
            </div>
            <h3 className="text-base font-semibold text-fd-foreground">
              One API
            </h3>
          </div>
          <p className="text-sm text-fd-muted-foreground leading-relaxed">
            Every driver plugs into the same{" "}
            <code className="text-xs bg-fd-muted/50 px-1.5 py-0.5 rounded font-mono">
              grove.Open(drv)
            </code>{" "}
            interface. Switch from PostgreSQL to ClickHouse without changing
            application code. Models, hooks, and migrations compose across all 7
            drivers.
          </p>

          <div className="mt-4 flex flex-wrap gap-2">
            {[...featuredDrivers, ...compactDrivers].map((d) => (
              <span
                key={d.abbr}
                className={cn(
                  "inline-flex items-center px-2 py-0.5 rounded text-[10px] font-mono font-medium border",
                  badgeColorMap[d.color],
                )}
              >
                {d.name}
              </span>
            ))}
          </div>
        </div>

        {/* Right: code */}
        <div className="border-t lg:border-t-0 lg:border-l border-fd-border p-5 flex items-center">
          <CodeBlock
            code={unifiedCode}
            filename="main.go"
            showLineNumbers={false}
            className="text-xs w-full"
          />
        </div>
      </div>
    </motion.div>
  );
}

// ─── Main Component ─────────────────────────────────────────

export function DriverGrid() {
  return (
    <section className="relative w-full py-20 sm:py-28">
      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <SectionHeader
          badge="Drivers"
          title="7 databases. One API."
          description="Each driver generates its database's native query syntax. PostgreSQL uses $1 placeholders, MySQL uses ?, MongoDB uses native BSON — no unified DSL compromising performance."
        />

        <motion.div
          variants={containerVariants}
          initial="hidden"
          whileInView="visible"
          viewport={{ once: true, margin: "-50px" }}
          className="mt-14 grid grid-cols-1 lg:grid-cols-2 gap-4"
        >
          {/* Row 1: PostgreSQL + MySQL */}
          <FeaturedCard driver={featuredDrivers[0]} />
          <FeaturedCard driver={featuredDrivers[1]} />

          {/* Row 2: MongoDB (left) + 4 compact drivers (right 2x2) */}
          <FeaturedCard driver={featuredDrivers[2]} />
          <div className="grid grid-cols-2 gap-4 content-start">
            {compactDrivers.map((d) => (
              <CompactCard key={d.abbr} driver={d} />
            ))}
          </div>

          {/* Row 3: Full-width unified API card */}
          <UnifiedAPICard />
        </motion.div>
      </div>
    </section>
  );
}
