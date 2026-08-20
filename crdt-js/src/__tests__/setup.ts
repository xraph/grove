import { afterEach } from "vitest";
import { cleanup } from "@testing-library/react";

// React 19 reads this to suppress the act() environment warning.
(globalThis as unknown as { IS_REACT_ACT_ENVIRONMENT: boolean })
  .IS_REACT_ACT_ENVIRONMENT = true;

afterEach(() => {
  cleanup();
});
