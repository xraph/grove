import { CodeShowcase } from "@/components/landing/code-showcase";
import { CTA } from "@/components/landing/cta";
import { DeliveryFlowSection } from "@/components/landing/delivery-flow-section";
import { FeatureBento } from "@/components/landing/feature-bento";
import { Hero } from "@/components/landing/hero";

export default function HomePage() {
  return (
    <main className="flex flex-col items-center overflow-x-hidden relative">
      <Hero />
      <FeatureBento />
      <DeliveryFlowSection />
      <CodeShowcase />
      <CTA />
    </main>
  );
}
