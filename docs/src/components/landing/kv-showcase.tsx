"use client";

import { motion } from "framer-motion";
import { CodeBlock } from "./code-block";
import { FeatureBullet } from "./feature-bullet";
import { FlowLine, FlowNode } from "./flow-primitives";
import { SectionHeader } from "./section-header";

// ─── KV Code Sample ──────────────────────────────────────────
const kvCode = `store := redis.New(redis.Config{Addr: ":6379"})

// Keyspaces for logical separation
cache := kv.WithKeyspace(store, "cache:")
sessions := kv.WithKeyspace(store, "session:")

cache.Set(ctx, "user:123", userData, 5*time.Minute)
val, _ := cache.Get(ctx, "user:123")`;

// ─── Keyspace Visualization ──────────────────────────────────
function KeyspaceVisualization() {
  const namespaces = [
    { label: "cache:", color: "blue" as const, delay: 0.1 },
    { label: "session:", color: "green" as const, delay: 0.2 },
    { label: "config:", color: "purple" as const, delay: 0.3 },
  ];

  const backends = [
    { label: "Redis", color: "blue" as const, delay: 0.4 },
    { label: "Badger", color: "green" as const, delay: 0.5 },
    { label: "DynamoDB", color: "orange" as const, delay: 0.6 },
  ];

  return (
    <motion.div
      initial={{ opacity: 0 }}
      whileInView={{ opacity: 1 }}
      viewport={{ once: true }}
      transition={{ duration: 0.6 }}
      className="relative"
    >
      {/* Background glow */}
      <div className="absolute inset-0 -m-6 bg-linear-to-br from-purple-500/5 via-transparent to-blue-500/5 rounded-3xl blur-xl" />

      <div className="relative p-3 sm:p-6 rounded-2xl border border-fd-border/50 bg-fd-card/30 backdrop-blur-sm">
        <div className="flex flex-col items-center gap-6">
          {/* Keyspace flow diagram */}
          <div className="flex flex-col gap-4 w-full">
            {namespaces.map((ns, i) => (
              <motion.div
                key={ns.label}
                initial={{ opacity: 0, x: -12 }}
                whileInView={{ opacity: 1, x: 0 }}
                viewport={{ once: true }}
                transition={{ duration: 0.4, delay: ns.delay }}
                className="flex items-center gap-0"
              >
                <FlowNode
                  label={ns.label}
                  color={ns.color}
                  size="sm"
                  pulse={i === 0}
                  delay={ns.delay}
                />
                <FlowLine length={48} color={ns.color} delay={ns.delay + 0.2} />
                <FlowNode
                  label={backends[i].label}
                  color={backends[i].color}
                  size="sm"
                  delay={backends[i].delay}
                />
              </motion.div>
            ))}
          </div>

          {/* Legend */}
          <div className="flex items-center gap-4 text-[10px] text-fd-muted-foreground">
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-blue-500" />
              <span>Cache</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-green-500" />
              <span>Sessions</span>
            </div>
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-purple-500" />
              <span>Config</span>
            </div>
          </div>
        </div>
      </div>

      {/* Code block below visualization */}
      <motion.div
        initial={{ opacity: 0, y: 12 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: true }}
        transition={{ duration: 0.5, delay: 0.4 }}
        className="mt-4"
      >
        <CodeBlock
          code={kvCode}
          filename="kv.go"
          showLineNumbers={false}
          className="text-xs"
        />
      </motion.div>
    </motion.div>
  );
}

// ─── KV Showcase Section ─────────────────────────────────────
export function KVShowcase() {
  return (
    <section className="relative w-full py-20 sm:py-28 overflow-hidden">
      {/* Background */}
      <div className="absolute inset-0 bg-linear-to-b from-transparent via-purple-500/2 to-transparent" />

      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <div className="grid gap-12 lg:grid-cols-2 lg:gap-16 items-center">
          {/* Left: Visual (mirrored from CRDT — visual on left) */}
          <div className="relative">
            <KeyspaceVisualization />
          </div>

          {/* Right: Text content */}
          <div className="flex flex-col">
            <SectionHeader
              badge="KV Store"
              title="Universal key-value storage."
              description="One interface, five production-grade backends. Swap between Redis, Memcached, DynamoDB, BoltDB, and Badger without changing application code."
              align="left"
            />

            <div className="mt-8 space-y-5">
              <FeatureBullet
                title="5 Backends"
                description="Redis, Memcached, DynamoDB, BoltDB, and Badger — swap implementations anytime without code changes."
                delay={0.2}
              />
              <FeatureBullet
                title="Keyspaces"
                description="Logical namespaces within a single store. Prefix-based isolation keeps cache, sessions, and config separated."
                delay={0.3}
              />
              <FeatureBullet
                title="Middleware"
                description="Composable middleware for logging, metrics, circuit breakers, and caching layers. Stack them as needed."
                delay={0.4}
              />
            </div>

            <motion.div
              initial={{ opacity: 0 }}
              whileInView={{ opacity: 1 }}
              viewport={{ once: true }}
              transition={{ delay: 0.5 }}
              className="mt-8"
            >
              <a
                href="/docs/kv/overview"
                className="inline-flex items-center gap-1 text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-500 transition-colors"
              >
                Explore KV Store
                <svg
                  className="size-3.5"
                  viewBox="0 0 16 16"
                  fill="none"
                  aria-hidden="true"
                >
                  <path
                    d="M6 4l4 4-4 4"
                    stroke="currentColor"
                    strokeWidth="1.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                </svg>
              </a>
            </motion.div>
          </div>
        </div>
      </div>
    </section>
  );
}
