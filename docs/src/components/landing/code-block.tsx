"use client";

import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/cn";

// HTML-escape a plain text string.
function esc(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

// Tokenize-then-render Go syntax highlighter.
// Pass 1: a single regex with alternation matches tokens in priority order.
// Pass 2: each token maps to an HTML span; unmatched text is plain (escaped).
function highlightGo(code: string): string {
  const goKeywords = new Set([
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
  ]);
  const goTypes = new Set([
    "string",
    "int",
    "int64",
    "float64",
    "bool",
    "error",
    "byte",
    "rune",
    "any",
  ]);

  // Groups: 1=comment, 2=string, 3=backtick-string, 4=word, 5=func-call (UpperWord before '(')
  const tokenRe =
    /(\/\/.*$)|("(?:[^"\\]|\\.)*")|(`[^`]*`)|(\b[a-zA-Z_]\w*(?:\.\w+)*\b)|\b([A-Z]\w*)\s*(?=\()/gm;

  let out = "";
  let last = 0;

  for (let m = tokenRe.exec(code); m !== null; m = tokenRe.exec(code)) {
    // Append any unmatched text before this token.
    if (m.index > last) {
      out += esc(code.slice(last, m.index));
    }
    last = m.index + m[0].length;

    if (m[1] != null) {
      // Comment
      out += `<span class="text-fd-muted-foreground/60 italic">${esc(m[1])}</span>`;
    } else if (m[2] != null) {
      // Double-quoted string
      out += `<span class="text-teal-400">${esc(m[2])}</span>`;
    } else if (m[3] != null) {
      // Backtick string
      out += `<span class="text-teal-400">${esc(m[3])}</span>`;
    } else if (m[4] != null) {
      const word = m[4];
      // Check for "context.Context" style compound types
      if (word === "context.Context") {
        out += `<span class="text-cyan-400">${esc(word)}</span>`;
      } else if (goKeywords.has(word)) {
        out += `<span class="text-purple-400 font-medium">${esc(word)}</span>`;
      } else if (goTypes.has(word)) {
        out += `<span class="text-cyan-400">${esc(word)}</span>`;
      } else if (
        /^[A-Z]/.test(word) &&
        code.slice(last).trimStart().startsWith("(")
      ) {
        // Uppercase word followed by '(' — function/method call
        out += `<span class="text-blue-400">${esc(word)}</span>`;
      } else {
        out += esc(word);
      }
    } else if (m[5] != null) {
      out += `<span class="text-blue-400">${esc(m[5])}</span>`;
    }
  }

  // Append remaining text.
  if (last < code.length) {
    out += esc(code.slice(last));
  }

  return out;
}

// Tokenize-then-render TSX/JSX syntax highlighter.
function highlightTSX(code: string): string {
  const tsxKeywords = new Set([
    "import",
    "export",
    "from",
    "const",
    "let",
    "var",
    "function",
    "return",
    "if",
    "else",
    "for",
    "while",
    "default",
    "new",
    "this",
    "class",
    "extends",
    "async",
    "await",
    "typeof",
    "instanceof",
    "null",
    "undefined",
    "true",
    "false",
  ]);

  // Tokenize: groups in priority order.
  // 1=comment, 2=double-string, 3=single-string, 4=backtick-string,
  // 5=arrow (=>), 6=JSX tag (<Tag or </Tag), 7=word, 8=curly block {…}
  const tokenRe =
    /(\/\/.*$)|("(?:[^"\\]|\\.)*")|('(?:[^'\\]|\\.)*')|(`[^`]*`)|(=>)|(<\/?)([\w.]+)|(\b[a-zA-Z_][\w]*\b)|(\{[^}]*\})/gm;

  let out = "";
  let last = 0;

  // Track whether we're inside a JSX tag (between < and >) for prop detection.
  for (let m = tokenRe.exec(code); m !== null; m = tokenRe.exec(code)) {
    if (m.index > last) {
      out += esc(code.slice(last, m.index));
    }
    last = m.index + m[0].length;

    if (m[1] != null) {
      // Comment
      out += `<span class="text-fd-muted-foreground/60 italic">${esc(m[1])}</span>`;
    } else if (m[2] != null) {
      // Double-quoted string
      out += `<span class="text-teal-400">${esc(m[2])}</span>`;
    } else if (m[3] != null) {
      // Single-quoted string
      out += `<span class="text-teal-400">${esc(m[3])}</span>`;
    } else if (m[4] != null) {
      // Backtick string
      out += `<span class="text-teal-400">${esc(m[4])}</span>`;
    } else if (m[5] != null) {
      // Arrow function =>
      out += `<span class="text-purple-400">${esc(m[5])}</span>`;
    } else if (m[6] != null && m[7] != null) {
      // JSX tag: <Tag or </Tag
      out += `${esc(m[6])}<span class="text-blue-400">${esc(m[7])}</span>`;
    } else if (m[8] != null) {
      const word = m[8];
      // Check if this word is followed by '=' (JSX prop)
      const afterWord = code.slice(last);
      if (afterWord.startsWith("=") && !afterWord.startsWith("==")) {
        out += `<span class="text-cyan-400">${esc(word)}</span>`;
      } else if (tsxKeywords.has(word)) {
        out += `<span class="text-purple-400 font-medium">${esc(word)}</span>`;
      } else {
        out += esc(word);
      }
    } else if (m[9] != null) {
      // Curly block {…} — highlight inner content
      const block = m[9];
      const inner = block.slice(1, -1);
      out += `{<span class="text-amber-300">${esc(inner)}</span>}`;
    }
  }

  if (last < code.length) {
    out += esc(code.slice(last));
  }

  return out;
}

interface CodeBlockProps {
  code: string;
  filename?: string;
  className?: string;
  showLineNumbers?: boolean;
  language?: "go" | "tsx";
}

export function CodeBlock({
  code,
  filename,
  className,
  showLineNumbers = true,
  language = "go",
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

  const highlighter = language === "tsx" ? highlightTSX : highlightGo;
  const lines = code.split("\n");
  const highlighted = lines.map((line) => highlighter(line));

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
