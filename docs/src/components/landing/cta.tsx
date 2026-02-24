"use client";

import { motion } from "framer-motion";
import Link from "next/link";
import { cn } from "@/lib/cn";

export function CTA() {
  return (
    <section className="relative w-full py-20 sm:py-28 overflow-hidden">
      {/* Background gradients */}
      <div className="absolute inset-0 bg-gradient-to-b from-transparent via-blue-500/[0.03] to-transparent" />
      <div className="absolute bottom-0 left-1/2 -translate-x-1/2 w-[600px] h-[300px] bg-gradient-to-t from-blue-500/8 to-transparent rounded-full blur-3xl" />

      <div className="relative container max-w-(--fd-layout-width) mx-auto px-4 sm:px-6">
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="max-w-2xl mx-auto text-center"
        >
          <h2 className="text-3xl font-bold tracking-tight text-fd-foreground sm:text-4xl">
            Build data-intensive Go apps
          </h2>
          <p className="mt-4 text-lg text-fd-muted-foreground leading-relaxed">
            Polyglot ORM, offline-first CRDT sync, universal KV store, and
            privacy hooks across 7 database drivers. Add Grove to your Go
            service in minutes.
          </p>

          {/* Install command */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="mt-8 flex items-center justify-center gap-2 rounded-lg border border-fd-border bg-fd-muted/40 px-4 py-2.5 font-mono text-sm max-w-md mx-auto"
          >
            <span className="text-fd-muted-foreground select-none">$</span>
            <code className="text-fd-foreground">
              go get github.com/xraph/grove
            </code>
          </motion.div>

          {/* CTAs */}
          <motion.div
            initial={{ opacity: 0, y: 12 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.3 }}
            className="mt-8 flex items-center justify-center gap-3"
          >
            <Link
              href="/docs"
              className={cn(
                "inline-flex items-center justify-center rounded-lg px-6 py-2.5 text-sm font-medium transition-colors",
                "bg-blue-500 text-white hover:bg-blue-600",
                "shadow-sm shadow-blue-500/20",
              )}
            >
              Get Started
            </Link>
            <Link
              href="/docs/guides/basic-crud"
              className={cn(
                "inline-flex items-center justify-center rounded-lg px-6 py-2.5 text-sm font-medium transition-colors",
                "border border-fd-border bg-fd-background hover:bg-fd-muted/50 text-fd-foreground",
              )}
            >
              View Examples
            </Link>
          </motion.div>
        </motion.div>
      </div>
    </section>
  );
}
