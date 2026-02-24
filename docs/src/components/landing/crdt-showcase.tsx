"use client";

import { motion } from "framer-motion";
import { CodeBlock } from "./code-block";
import { FeatureBullet } from "./feature-bullet";
import { FlowLine, FlowNode } from "./flow-primitives";
import { SectionHeader } from "./section-header";

// ─── CRDT Code Example ──────────────────────────────────────
const crdtCode = `plugin := crdt.NewPlugin(crdt.PluginConfig{
    NodeID:    "node-1",
    Tombstone: true,
})
plugin.Register("documents", crdt.LWWRegister)
plugin.Register("likes",     crdt.PNCounter)
plugin.Register("tags",      crdt.ORSet)`;

// ─── Replica Sync Icon ──────────────────────────────────────
function SyncIcon({ className }: { className?: string }) {
  return (
    <svg
      className={className}
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
      <circle cx="6" cy="6" r="2" stroke="currentColor" strokeWidth="1.5" />
    </svg>
  );
}

// ─── Replica Visualization ──────────────────────────────────
function ReplicaDiagram() {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      whileInView={{ opacity: 1 }}
      viewport={{ once: true }}
      transition={{ duration: 0.6 }}
      className="relative"
    >
      {/* Background glow */}
      <div className="absolute inset-0 -m-6 bg-linear-to-br from-green-500/5 via-transparent to-teal-500/5 rounded-3xl blur-xl" />

      <div className="relative p-3 sm:p-6 rounded-2xl border border-fd-border/50 bg-fd-card/30 backdrop-blur-sm">
        <div className="flex flex-col items-center gap-6">
          {/* Replica nodes row */}
          <div className="flex items-center gap-0">
            <FlowNode
              label="Replica A"
              color="green"
              size="sm"
              pulse
              delay={0.2}
              icon={<SyncIcon className="size-3" />}
            />
            <FlowLine length={40} color="green" delay={1} />
            <FlowNode
              label="Replica B"
              color="green"
              size="sm"
              pulse
              delay={0.4}
              icon={<SyncIcon className="size-3" />}
            />
            <FlowLine length={40} color="green" delay={2} />
            <FlowNode
              label="Replica C"
              color="green"
              size="sm"
              pulse
              delay={0.6}
              icon={<SyncIcon className="size-3" />}
            />
          </div>

          {/* Convergence status */}
          <div className="flex items-center gap-4 text-[10px] text-fd-muted-foreground">
            <div className="flex items-center gap-1.5">
              <div className="size-2 rounded-full bg-green-500" />
              <span>Converged</span>
            </div>
            <div className="flex items-center gap-1.5">
              <motion.div
                className="size-2 rounded-full bg-green-400"
                animate={{ opacity: [1, 0.4, 1] }}
                transition={{ duration: 2, repeat: Infinity }}
              />
              <span>Syncing</span>
            </div>
          </div>

          {/* Code block */}
          <div className="w-full">
            <CodeBlock
              code={crdtCode}
              filename="crdt.go"
              showLineNumbers={false}
              className="text-xs"
            />
          </div>
        </div>
      </div>
    </motion.div>
  );
}

// ─── CRDT Showcase Section ──────────────────────────────────
export function CRDTShowcase() {
  return (
    <section className="relative w-full py-20 sm:py-28 overflow-hidden">
      {/* Background */}
      <div className="absolute inset-0 bg-linear-to-b from-transparent via-green-500/2 to-transparent" />

      <div className="container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <div className="grid gap-12 lg:grid-cols-2 lg:gap-16 items-center">
          {/* Left: Text content */}
          <div className="flex flex-col">
            <SectionHeader
              badge="CRDT"
              title="Offline-first by design."
              description="Conflict-Free Replicated Data Types let distributed nodes modify data independently and converge automatically — no coordination required."
              align="left"
            />

            <div className="mt-8 space-y-5">
              <FeatureBullet
                title="LWW-Register"
                description="Last-Writer-Wins for simple fields. Automatic conflict resolution using logical timestamps across replicas."
                delay={0.2}
              />
              <FeatureBullet
                title="PN-Counter"
                description="Distributed counters that always converge. Increment and decrement independently on any node."
                delay={0.3}
              />
              <FeatureBullet
                title="OR-Set"
                description="Observed-Remove sets that handle concurrent add/remove operations across replicas without conflicts."
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
                href="/docs/crdt/overview"
                className="inline-flex items-center gap-1 text-sm font-medium text-blue-600 dark:text-blue-400 hover:text-blue-500 transition-colors"
              >
                Learn about CRDT
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

          {/* Right: Replica visualization */}
          <div className="relative">
            <ReplicaDiagram />
          </div>
        </div>
      </div>
    </section>
  );
}
