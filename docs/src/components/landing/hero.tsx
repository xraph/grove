"use client";

import { motion } from "framer-motion";
import Link from "next/link";
import { cn } from "@/lib/cn";
import { AnimatedTagline } from "./animated-tagline";
import { FloatingBadge, FlowLine, FlowNode } from "./flow-primitives";

function GitHubIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
      fill="currentColor"
      viewBox="0 0 24 24"
      aria-hidden="true"
    >
      <path d="M12 .297c-6.63 0-12 5.373-12 12 0 5.303 3.438 9.8 8.205 11.385.6.113.82-.258.82-.577 0-.285-.01-1.04-.015-2.04-3.338.724-4.042-1.61-4.042-1.61C4.422 18.07 3.633 17.7 3.633 17.7c-1.087-.744.084-.729.084-.729 1.205.084 1.838 1.236 1.838 1.236 1.07 1.835 2.809 1.305 3.495.998.108-.776.417-1.305.76-1.605-2.665-.3-5.466-1.332-5.466-5.93 0-1.31.465-2.38 1.235-3.286-.135-.303-.54-1.523.105-3.176 0 0 1.005-.322 3.3 1.23.96-.267 1.98-.399 3-.405 1.02.006 2.04.138 3 .405 2.28-1.552 3.285-1.23 3.285-1.23.645 1.653.24 2.873.12 3.176.765.84 1.23 1.91 1.23 3.22 0 4.61-2.805 5.625-5.475 5.92.42.36.81 1.096.81 2.22 0 1.606-.015 2.896-.015 3.286 0 .315.21.69.825.57C20.565 22.092 24 17.592 24 12.297c0-6.627-5.373-12-12-12" />
    </svg>
  );
}

// ─── Ecosystem Diagram ──────────────────────────────────────
function EcosystemDiagram() {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.6, delay: 0.3 }}
      className="relative w-full max-w-md mx-auto"
    >
      {/* Background glow */}
      <div className="absolute inset-0 -m-8 bg-linear-to-br from-blue-500/5 via-transparent to-indigo-500/5 rounded-3xl blur-2xl" />

      <div className="relative space-y-6 p-4">
        {/* Row 1: Grove center node */}
        <div className="flex items-center justify-center">
          <FlowNode
            label="Grove"
            color="blue"
            size="sm"
            pulse
            delay={0.4}
            icon={
              <svg
                className="size-3"
                viewBox="0 0 12 12"
                fill="none"
                aria-hidden="true"
              >
                <path
                  d="M6 1L1 4v4l5 3 5-3V4L6 1z"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinejoin="round"
                />
              </svg>
            }
          />
        </div>

        {/* Row 2: Three pillars — ORM, CRDT, KV */}
        <div className="flex items-center justify-center gap-0">
          <FlowNode
            label="ORM"
            color="blue"
            size="sm"
            delay={0.55}
            icon={
              <svg
                className="size-3"
                viewBox="0 0 12 12"
                fill="none"
                aria-hidden="true"
              >
                <ellipse
                  cx="6"
                  cy="3.5"
                  rx="4"
                  ry="2"
                  stroke="currentColor"
                  strokeWidth="1.5"
                />
                <path
                  d="M2 3.5v5c0 1.1 1.8 2 4 2s4-.9 4-2v-5"
                  stroke="currentColor"
                  strokeWidth="1.5"
                />
              </svg>
            }
          />
          <FlowLine length={20} color="blue" delay={1} />
          <FlowNode
            label="CRDT"
            color="green"
            size="sm"
            delay={0.7}
            icon={
              <svg
                className="size-3"
                viewBox="0 0 12 12"
                fill="none"
                aria-hidden="true"
              >
                <path
                  d="M1 6h3M8 6h3M6 1v3M6 8v3"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                />
                <circle
                  cx="6"
                  cy="6"
                  r="2"
                  stroke="currentColor"
                  strokeWidth="1.5"
                />
              </svg>
            }
          />
          <FlowLine length={20} color="green" delay={2} />
          <FlowNode
            label="KV Store"
            color="purple"
            size="sm"
            delay={0.85}
            icon={
              <svg
                className="size-3"
                viewBox="0 0 12 12"
                fill="none"
                aria-hidden="true"
              >
                <path
                  d="M4 3h5M4 6h5M4 9h3"
                  stroke="currentColor"
                  strokeWidth="1.5"
                  strokeLinecap="round"
                />
                <circle cx="2" cy="3" r="1" fill="currentColor" />
                <circle cx="2" cy="6" r="1" fill="currentColor" />
                <circle cx="2" cy="9" r="1" fill="currentColor" />
              </svg>
            }
          />
        </div>

        {/* Row 3: Pipeline events */}
        <div className="flex items-start justify-center">
          <div className="space-y-2.5">
            <motion.div
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 1.0 }}
              className="flex items-center gap-0"
            >
              <FlowLine length={28} color="blue" delay={3} />
              <FlowNode
                label="query.build"
                color="gray"
                size="sm"
                delay={1.1}
              />
              <FlowLine length={24} color="blue" delay={4} />
              <div className="rounded-md border border-green-500/20 bg-green-500/10 px-2 py-0.5 font-mono text-[10px] font-medium text-green-600 dark:text-green-400 whitespace-nowrap">
                native SQL
              </div>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 1.2 }}
              className="flex items-center gap-0"
            >
              <FlowLine length={28} color="green" delay={5} />
              <FlowNode label="crdt.sync" color="gray" size="sm" delay={1.3} />
              <FlowLine length={24} color="green" delay={6} />
              <div className="rounded-md border border-green-500/20 bg-green-500/10 px-2 py-0.5 font-mono text-[10px] font-medium text-green-600 dark:text-green-400 whitespace-nowrap">
                converged
              </div>
            </motion.div>

            <motion.div
              initial={{ opacity: 0, x: -10 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 1.4 }}
              className="flex items-center gap-0"
            >
              <FlowLine length={28} color="purple" delay={7} />
              <FlowNode label="kv.get" color="gray" size="sm" delay={1.5} />
              <FlowLine length={24} color="purple" delay={8} />
              <div className="rounded-md border border-green-500/20 bg-green-500/10 px-2 py-0.5 font-mono text-[10px] font-medium text-green-600 dark:text-green-400 whitespace-nowrap">
                cache hit
              </div>
            </motion.div>
          </div>
        </div>

        {/* Floating driver badges */}
        <div className="flex flex-wrap items-center justify-center gap-2 pt-2">
          <FloatingBadge label="PostgreSQL" delay={1.6} />
          <FloatingBadge label="MySQL" delay={1.7} />
          <FloatingBadge label="MongoDB" delay={1.8} />
          <FloatingBadge label="SQLite" delay={1.9} />
          <FloatingBadge label="Turso" delay={2.0} />
          <FloatingBadge label="ClickHouse" delay={2.1} />
          <FloatingBadge label="Elasticsearch" delay={2.2} />
        </div>
      </div>
    </motion.div>
  );
}

// ─── Hero Section ────────────────────────────────────────────
export function Hero() {
  return (
    <section className="relative w-full overflow-hidden">
      {/* Dotted background */}
      <div className="absolute inset-0 bg-dotted opacity-40 dark:opacity-20" />

      {/* Radial gradient overlays */}
      <div className="absolute inset-0 bg-linear-to-b from-fd-background via-transparent to-fd-background" />
      <div className="absolute top-0 left-1/2 -translate-x-1/2 w-[800px] h-[600px] bg-linear-to-b from-blue-500/8 to-transparent rounded-full blur-3xl" />

      <div className="relative container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <div className="grid gap-12 lg:grid-cols-2 lg:gap-16 items-center py-20 sm:py-28 md:py-32">
          {/* Left: Text content */}
          <div className="flex flex-col items-start">
            {/* Pill badge */}
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.4 }}
            >
              <span className="inline-flex items-center rounded-full border border-blue-500/20 bg-blue-500/10 px-3.5 py-1 text-xs font-medium text-blue-600 dark:text-blue-400 mb-6">
                The Complete Go Data Toolkit
              </span>
            </motion.div>

            {/* Animated headline */}
            <AnimatedTagline />

            {/* Description */}
            <motion.p
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.6 }}
              className="mt-6 text-lg text-fd-muted-foreground leading-relaxed max-w-lg"
            >
              Polyglot ORM with native query syntax, offline-first CRDT sync,
              universal KV store, and privacy hooks &mdash; across 7 database
              drivers. Part of the Forge ecosystem.
            </motion.p>

            {/* Install command */}
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 0.8 }}
              className="mt-6 flex items-center gap-2 rounded-lg border border-fd-border bg-fd-muted/40 px-4 py-2.5 font-mono text-sm"
            >
              <span className="text-fd-muted-foreground select-none">$</span>
              <code className="text-fd-foreground">
                go get github.com/xraph/grove
              </code>
            </motion.div>

            {/* CTAs */}
            <motion.div
              initial={{ opacity: 0, y: 12 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ duration: 0.5, delay: 1.0 }}
              className="mt-8 flex items-center gap-3"
            >
              <Link
                href="/docs"
                className={cn(
                  "inline-flex items-center justify-center rounded-lg px-5 py-2.5 text-sm font-medium transition-colors",
                  "bg-blue-500 text-white hover:bg-blue-600",
                  "shadow-sm shadow-blue-500/20",
                )}
              >
                Get Started
              </Link>
              <a
                href="https://github.com/xraph/grove"
                target="_blank"
                rel="noreferrer"
                className={cn(
                  "inline-flex items-center gap-2 justify-center rounded-lg px-5 py-2.5 text-sm font-medium transition-colors",
                  "border border-fd-border bg-fd-background hover:bg-fd-muted/50 text-fd-foreground",
                )}
              >
                <GitHubIcon className="size-4" />
                GitHub
              </a>
            </motion.div>
          </div>

          {/* Right: Ecosystem diagram */}
          <div className="relative lg:pl-8">
            <EcosystemDiagram />
          </div>
        </div>
      </div>
    </section>
  );
}
