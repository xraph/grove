"use client";

export function ThemedLogo() {
  return (
    <div className="relative flex items-center justify-center size-8">
      <svg
        viewBox="0 0 32 32"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className="size-8"
        aria-hidden="true"
      >
        {/* Arrow/relay symbol */}
        <rect
          x="2"
          y="2"
          width="28"
          height="28"
          rx="6"
          className="fill-teal-500 dark:fill-teal-400"
        />
        <path
          d="M10 16L15 11L15 14L22 14L22 18L15 18L15 21L10 16Z"
          className="fill-white"
        />
        <circle cx="8" cy="16" r="1.5" className="fill-white/60" />
        <circle cx="24" cy="12" r="1.5" className="fill-white/60" />
        <circle cx="24" cy="20" r="1.5" className="fill-white/60" />
      </svg>
    </div>
  );
}
