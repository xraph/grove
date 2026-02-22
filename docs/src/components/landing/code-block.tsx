"use client";

import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/cn";

// Simple Go syntax highlighter
function highlightGo(code: string): string {
  // Order matters: do strings first to avoid highlighting keywords inside strings
  let result = code;

  // Escape HTML first
  result = result
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");

  // Comments (single-line)
  result = result.replace(
    /(\/\/.*$)/gm,
    '<span className="text-fd-muted-foreground/60 italic">$1</span>',
  );

  // Strings (double-quoted)
  result = result.replace(
    /("(?:[^"\\]|\\.)*")/g,
    '<span className="text-teal-400">$1</span>',
  );

  // Backtick strings
  result = result.replace(
    /(`[^`]*`)/g,
    '<span className="text-teal-400">$1</span>',
  );

  // Keywords
  const keywords = [
    "package",
    "import",
    "func",
    "return",
    "if",
    "else",
    "for",
    "range",
    "var",
    "const",
    "type",
    "struct",
    "interface",
    "map",
    "chan",
    "go",
    "defer",
    "select",
    "case",
    "switch",
    "default",
    "break",
    "continue",
    "fallthrough",
    "nil",
    "true",
    "false",
    "err",
  ];
  keywords.forEach((kw) => {
    const regex = new RegExp(`\\b(${kw})\\b`, "g");
    result = result.replace(
      regex,
      '<span class="text-purple-400 font-medium">$1</span>',
    );
  });

  // Types
  const types = [
    "string",
    "int",
    "int64",
    "float64",
    "bool",
    "error",
    "byte",
    "rune",
    "any",
    "context\\.Context",
  ];
  types.forEach((t) => {
    const regex = new RegExp(`\\b(${t})\\b`, "g");
    result = result.replace(regex, '<span class="text-cyan-400">$1</span>');
  });

  // Function calls
  result = result.replace(
    /\b([A-Z]\w*)\s*\(/g,
    '<span class="text-blue-400">$1</span>(',
  );

  // Method calls (after dot)
  result = result.replace(
    /\.([A-Z]\w*)\s*\(/g,
    '.<span class="text-blue-400">$1</span>(',
  );

  return result;
}

interface CodeBlockProps {
  code: string;
  filename?: string;
  className?: string;
  showLineNumbers?: boolean;
}

export function CodeBlock({
  code,
  filename,
  className,
  showLineNumbers = true,
}: CodeBlockProps) {
  const [copied, setCopied] = useState(false);
  const codeRef = useRef<HTMLPreElement>(null);

  useEffect(() => {
    if (copied) {
      const timeout = setTimeout(() => setCopied(false), 2000);
      return () => clearTimeout(timeout);
    }
  }, [copied]);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
  };

  const lines = code.split("\n");
  const highlighted = lines.map((line) => highlightGo(line));

  return (
    <div
      className={cn(
        "relative rounded-xl border border-fd-border bg-fd-background/50 backdrop-blur-sm overflow-hidden",
        className,
      )}
    >
      {/* Header bar */}
      {filename && (
        <div className="flex items-center justify-between px-4 py-2.5 border-b border-fd-border bg-fd-muted/30">
          <div className="flex items-center gap-2">
            <div className="flex gap-1.5">
              <div className="size-2.5 rounded-full bg-fd-muted-foreground/20" />
              <div className="size-2.5 rounded-full bg-fd-muted-foreground/20" />
              <div className="size-2.5 rounded-full bg-fd-muted-foreground/20" />
            </div>
            <span className="text-xs text-fd-muted-foreground font-mono ml-2">
              {filename}
            </span>
          </div>
          <button
            type="button"
            onClick={handleCopy}
            className="text-xs text-fd-muted-foreground hover:text-fd-foreground transition-colors px-2 py-1 rounded-md hover:bg-fd-muted/50"
          >
            {copied ? "Copied!" : "Copy"}
          </button>
        </div>
      )}

      {/* Code content */}
      <pre
        ref={codeRef}
        className="overflow-x-auto p-4 text-[13px] leading-relaxed font-mono"
      >
        <code>
          {highlighted.map((line, i) => (
            // biome-ignore lint/suspicious/noArrayIndexKey: static code lines never reorder
            <div key={i} className="flex">
              {showLineNumbers && (
                <span className="select-none text-fd-muted-foreground/30 w-8 shrink-0 text-right pr-4 text-xs leading-relaxed">
                  {i + 1}
                </span>
              )}
              <span
                className="flex-1"
                // biome-ignore lint/security/noDangerouslySetInnerHtml: syntax highlighted code from static input
                dangerouslySetInnerHTML={{ __html: line || "&nbsp;" }}
              />
            </div>
          ))}
        </code>
      </pre>
    </div>
  );
}
