import { CodeShowcase } from "@/components/landing/code-showcase";
import { CRDTShowcase } from "@/components/landing/crdt-showcase";
import { CTA } from "@/components/landing/cta";
import { DeliveryFlowSection } from "@/components/landing/delivery-flow-section";
import { DriverGrid } from "@/components/landing/driver-grid";
import { FeatureBento } from "@/components/landing/feature-bento";
import { Hero } from "@/components/landing/hero";
import { KVShowcase } from "@/components/landing/kv-showcase";

export default function HomePage() {
  return (
    <main className="flex flex-col items-center overflow-x-hidden relative">
      <Hero />
      <DriverGrid />
      <FeatureBento />
      <CRDTShowcase />
      <KVShowcase />
      <DeliveryFlowSection />
      <CodeShowcase />
      <CTA />
    </main>
  );
}
