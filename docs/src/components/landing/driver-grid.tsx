"use client";

import { motion } from "framer-motion";
import { cn } from "@/lib/cn";
import { SectionHeader } from "./section-header";

interface Driver {
  abbr: string;
  name: string;
  description: string;
  color: "blue" | "orange" | "teal" | "green" | "purple" | "amber" | "indigo";
}

const drivers: Driver[] = [
  {
    abbr: "PG",
    name: "PostgreSQL",
    description: "Native $1 placeholders, JSONB operators, and DISTINCT ON support.",
    color: "blue",
  },
  {
    abbr: "MY",
    name: "MySQL",
    description: "Backtick quoting, ? placeholders, and ON DUPLICATE KEY upserts.",
    color: "orange",
  },
  {
    abbr: "SQ",
    name: "SQLite",
    description: "Lightweight embedded storage with full SQL and WAL mode.",
    color: "teal",
  },
  {
    abbr: "MG",
    name: "MongoDB",
    description: "Native BSON queries, aggregation pipelines, and change streams.",
    color: "green",
  },
  {
    abbr: "TU",
    name: "Turso",
    description: "Edge-replicated SQLite with libSQL and embedded replicas.",
    color: "purple",
  },
  {
    abbr: "CH",
    name: "ClickHouse",
    description: "Columnar analytics with native protocol and batch inserts.",
    color: "amber",
  },
  {
    abbr: "ES",
    name: "Elasticsearch",
    description: "Full-text search with native JSON DSL and bulk indexing.",
    color: "indigo",
  },
];

const iconColorMap = {
  blue: "bg-blue-500/10 text-blue-600 dark:text-blue-400",
  orange: "bg-orange-500/10 text-orange-600 dark:text-orange-400",
  teal: "bg-teal-500/10 text-teal-600 dark:text-teal-400",
  green: "bg-green-500/10 text-green-600 dark:text-green-400",
  purple: "bg-purple-500/10 text-purple-600 dark:text-purple-400",
  amber: "bg-amber-500/10 text-amber-600 dark:text-amber-400",
  indigo: "bg-indigo-500/10 text-indigo-600 dark:text-indigo-400",
};

const hoverBorderMap = {
  blue: "hover:border-blue-500/20",
  orange: "hover:border-orange-500/20",
  teal: "hover:border-teal-500/20",
  green: "hover:border-green-500/20",
  purple: "hover:border-purple-500/20",
  amber: "hover:border-amber-500/20",
  indigo: "hover:border-indigo-500/20",
};

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

// Split drivers into grid rows: first 4 in the grid, last 3 centered
const gridDrivers = drivers.slice(0, 4);
const centeredDrivers = drivers.slice(4);

function DriverCard({ driver }: { driver: Driver }) {
  return (
    <motion.div
      variants={itemVariants}
      className={cn(
        "rounded-xl border border-fd-border bg-fd-card/50 backdrop-blur-sm p-5 transition-all duration-300",
        hoverBorderMap[driver.color],
      )}
    >
      <div
        className={cn(
          "flex items-center justify-center size-10 rounded-lg font-mono text-sm font-bold mb-3",
          iconColorMap[driver.color],
        )}
      >
        {driver.abbr}
      </div>
      <h3 className="text-sm font-semibold text-fd-foreground">
        {driver.name}
      </h3>
      <p className="text-xs text-fd-muted-foreground mt-1 leading-relaxed">
        {driver.description}
      </p>
    </motion.div>
  );
}

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
          className="mt-14"
        >
          {/* Top row: responsive grid */}
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
            {gridDrivers.map((driver) => (
              <DriverCard key={driver.abbr} driver={driver} />
            ))}
          </div>

          {/* Bottom row: centered */}
          <div className="flex flex-wrap justify-center gap-4 mt-4">
            {centeredDrivers.map((driver) => (
              <div key={driver.abbr} className="w-[calc(50%-0.5rem)] sm:w-[calc(33.333%-0.75rem)] lg:w-[calc(25%-0.75rem)]">
                <DriverCard driver={driver} />
              </div>
            ))}
          </div>
        </motion.div>
      </div>
    </section>
  );
}
