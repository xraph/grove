"use client";

import { motion } from "framer-motion";

export function FeatureBullet({
  title,
  description,
  delay,
}: {
  title: string;
  description: string;
  delay: number;
}) {
  return (
    <motion.div
      initial={{ opacity: 0, x: -10 }}
      whileInView={{ opacity: 1, x: 0 }}
      viewport={{ once: true }}
      transition={{ duration: 0.4, delay }}
      className="flex items-start gap-3"
    >
      <div className="mt-1 flex items-center justify-center size-5 rounded-md bg-blue-500/10 shrink-0">
        <svg
          className="size-3 text-blue-500"
          viewBox="0 0 12 12"
          fill="none"
          aria-hidden="true"
        >
          <path
            d="M2 6l3 3 5-5"
            stroke="currentColor"
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </div>
      <div>
        <h4 className="text-sm font-semibold text-fd-foreground">{title}</h4>
        <p className="text-xs text-fd-muted-foreground mt-0.5 leading-relaxed">
          {description}
        </p>
      </div>
    </motion.div>
  );
}
