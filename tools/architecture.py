#!/usr/bin/env python3
# Draws the picture in the README, docs/architecture-{light,dark}.svg: a schematic of two nodes, their daemons, the hub
# and the sync stream between them, with what each part holds. Only right-angled wires.
# Usage: python3 tools/architecture.py [folder, default docs]
import sys
from html import escape

PALETTES = {
    "light": dict(bg="#ffffff", grid="#eef1f4", text="#1f2328", muted="#656d76", line="#8c959f", frame="#afb8c1",
                  bar="#f3f5f7", daemon="#0969da", hub="#1a7f37", link="#1a7f37", key="#8250df"),
    "dark": dict(bg="#0d1117", grid="#161b22", text="#e6edf3", muted="#8b949e", line="#6e7681", frame="#3d444d",
                 bar="#161b22", daemon="#4493f8", hub="#3fb950", link="#3fb950", key="#d2a8ff"),
}
MONO = "ui-monospace, SFMono-Regular, 'SF Mono', Menlo, Consolas, 'Liberation Mono', monospace"
W, H = 980, 560


def svg(p):
    out = []
    a = out.append

    def text(x, y, s, color=None, size=12, anchor="start", weight="400"):
        a(f'<text x="{x}" y="{y}" font-size="{size}" font-weight="{weight}" text-anchor="{anchor}" '
          f'fill="{color or p["text"]}">{escape(s)}</text>')

    def head(x, y, dx, dy, color, size=8):  # an arrowhead with its tip at (x, y), pointing along (dx, dy)
        bx, by = x - dx * size, y - dy * size
        px, py = -dy * size * 0.45, dx * size * 0.45
        a(f'<path d="M {x} {y} L {bx + px} {by + py} L {bx - px} {by - py} z" fill="{color}"/>')

    def wire(points, color, width=1.6, dash=None, start=False, end=True):  # a right-angled line through the points
        d = " ".join(("M" if i == 0 else "L") + f" {x} {y}" for i, (x, y) in enumerate(points))
        dash = f' stroke-dasharray="{dash}"' if dash else ""
        a(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}"{dash}/>')
        for (x1, y1), (x2, y2), on in ((points[-2], points[-1], end), (points[1], points[0], start)):
            if on:
                n = abs(x2 - x1) + abs(y2 - y1)
                head(x2, y2, (x2 - x1) / n, (y2 - y1) / n, color)

    def block(x, y, w, title, accent, rows, note=None):  # a box with a title bar; rows are (key, value)
        h = 34 + 21 * len(rows) + 10
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="3" fill="{p["bg"]}" stroke="{accent}" stroke-width="1.4"/>')
        a(f'<path d="M {x} {y + 3} a 3 3 0 0 1 3 -3 h {w - 6} a 3 3 0 0 1 3 3 v 23 h {-w} z" fill="{p["bar"]}"/>')
        a(f'<line x1="{x}" y1="{y + 26}" x2="{x + w}" y2="{y + 26}" stroke="{accent}" stroke-width="1"/>')
        text(x + 10, y + 18, title, accent, 12.5, weight="700")
        if note:
            text(x + w - 10, y + 18, note, p["muted"], 11, "end")
        for i, (k, v) in enumerate(rows):
            yy = y + 48 + 21 * i
            if k == "$":  # a command line
                a(f'<text x="{x + 12}" y="{yy}" font-size="11.5" fill="{p["key"]}">$ <tspan fill="{p["text"]}">{escape(v)}</tspan></text>')
                continue
            text(x + 12, yy, k, p["key"], 11.5)
            text(x + 84, yy, v, p["text"], 11.5)
        return h

    def node(x, y, w, h, name, role):  # the dashed outline of one machine
        a(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="4" fill="none" stroke="{p["frame"]}" '
          f'stroke-width="1.2" stroke-dasharray="6 4"/>')
        a(f'<rect x="{x + 12}" y="{y - 10}" width="{7.4 * len("node " + name + " " + role) + 16}" height="20" fill="{p["bg"]}"/>')
        a(f'<text x="{x + 20}" y="{y + 4}" font-size="12.5" fill="{p["muted"]}">node <tspan fill="{p["text"]}" '
          f'font-weight="700">{escape(name)}</tspan> {escape(role)}</text>')

    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" font-family="{MONO}" xml:space="preserve">')
    a(f'<defs><pattern id="grid" width="20" height="20" patternUnits="userSpaceOnUse">'
      f'<path d="M 20 0 L 0 0 0 20" fill="none" stroke="{p["grid"]}" stroke-width="1"/></pattern></defs>')
    a(f'<rect width="{W}" height="{H}" fill="{p["bg"]}"/><rect width="{W}" height="{H}" fill="url(#grid)"/>')

    # ── node mac ──
    node(20, 30, 360, 452, "mac", "· laptop")
    block(40, 58, 320, "clients", p["line"], [
        ("$", "kharka put tasks/1 '{\"title\":\"…\"}'"),
        ("$", "kharka watch status/ --initial"),
        ("$", "your-tool       # any program"),
    ])
    wire([(200, 156), (200, 218)], p["line"], start=True)
    text(212, 180, "unix socket · JSON lines", p["muted"], 11)
    text(212, 196, "…/kharka/daemon.sock", p["muted"], 10.5)
    block(40, 222, 320, "kharka daemon", p["daemon"], [
        ("store", "BTreeMap<key, entry>  in RAM"),
        ("log", "data.log  [len|crc32|json]…"),
        ("pending", "local writes, seq = 0"),
        ("clock", "hlc 1791573241742-0-mac"),
        ("scope", "everything | --sync tasks/"),
    ], "replica")
    text(40, 400, "reads   from memory, also offline", p["muted"], 11)
    text(40, 418, "writes  log first, then memory, then the hub", p["muted"], 11)
    text(40, 436, "watch   every change under a prefix", p["muted"], 11)

    # ── node server ──
    node(600, 30, 360, 452, "server", "· runs the hub")
    block(620, 58, 320, "kharka serve", p["hub"], [
        ("log", "seq 1…n, one entry per key"),
        ("order", "latest write wins (hlc)"),
        ("rules", "_collections/* ← kharka.toml"),
        ("nodes", "_nodes/<node>  online|offline"),
        ("refused", "_rejected/<node>/…"),
    ], "hub")
    wire([(780, 222), (780, 266)], p["hub"], start=True)
    text(792, 249, "hub.sock", p["muted"], 11)
    block(620, 270, 320, "kharka daemon", p["daemon"], [
        ("store", "BTreeMap<key, entry>  in RAM"),
        ("clock", "hlc 1791573241911-0-server"),
    ], "replica")
    wire([(780, 370), (780, 404)], p["line"], start=True)
    text(792, 392, "daemon.sock", p["muted"], 11)
    a(f'<rect x="620" y="408" width="320" height="30" rx="3" fill="{p["bg"]}" stroke="{p["line"]}" stroke-width="1.2"/>')
    text(632, 428, "$ kharka get tasks/1", p["text"], 11.5)

    # ── the stream: writes up, changes down, right angles only ──
    wire([(360, 280), (466, 280), (466, 124), (620, 124)], p["link"], 1.8)
    wire([(620, 150), (494, 150), (494, 306), (360, 306)], p["link"], 1.8)
    text(476, 116, "push", p["link"], 11.5, weight="700")
    text(504, 172, "changes", p["link"], 11.5, weight="700")

    text(490, 352, "ssh -T server kharka bridge", p["text"], 11, "middle")
    text(490, 369, "frames: u32 length + json", p["muted"], 11, "middle")
    frames = [
        ("→", "hello     {node,epoch,since}"),
        ("←", "welcome   {epoch,reset}"),
        ("←", "changes   [seq > since]"),
        ("←", "caught_up {seq}"),
        ("→", "push      [pending]"),
        ("←", "changes   live, to all"),
    ]
    for i, (arrow, frame) in enumerate(frames):
        text(394, 396 + 17 * i, arrow, p["link"], 11)
        text(410, 396 + 17 * i, frame, p["text"], 11)

    # ── more nodes, and the rules of the game ──
    wire([(780, 482), (780, 516), (730, 516)], p["frame"], 1.2, dash="4 4", end=False)
    text(722, 520, "more nodes · same hub", p["muted"], 11, "end")
    text(20, 546, "local-first · full replica per node · last write wins per key · deletions as tombstones · protocol v1",
         p["muted"], 11)
    a("</svg>")
    return "\n".join(out) + "\n"


folder = sys.argv[1] if len(sys.argv) > 1 else "docs"
for mode in ("light", "dark"):
    with open(f"{folder}/architecture-{mode}.svg", "w") as f:
        f.write(svg(PALETTES[mode]))
