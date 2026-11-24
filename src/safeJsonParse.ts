/**
 * `JSON.parse` without prototype poisoning, for JSON a client sent.
 *
 * `JSON.parse('{"__proto__":{"isAdmin":true}}')` creates an OWN `__proto__`
 * property. It is harmless while it stays there, but the first
 * `Object.assign`, `target[key] = source[key]` copy or deep merge that
 * reaches it runs the `__proto__` setter and swaps the target's prototype
 * for the client's data. A `constructor` object carrying `prototype` is the
 * other door merge utilities walk through.
 *
 * Both are removed after parsing, at any depth, in one iterative walk — no
 * recursion, so a deeply nested message cannot overflow the stack. A
 * `constructor` key holding a plain value (`{"constructor":"Ford"}`) is data,
 * and stays.
 *
 * Relay keeps its own copy: `@c9up/ream` is an optional peer, and its parser
 * is not part of what it exports.
 *
 * Throws the `SyntaxError` of `JSON.parse` on malformed input.
 */
export function safeJsonParse(text: string): unknown {
	const parsed: unknown = JSON.parse(text);
	if (parsed === null || typeof parsed !== "object") return parsed;
	const pending: object[] = [parsed];
	for (let node = pending.pop(); node !== undefined; node = pending.pop()) {
		if (Object.hasOwn(node, "__proto__")) {
			Reflect.deleteProperty(node, "__proto__");
		}
		const ctor: unknown = Object.hasOwn(node, "constructor")
			? Reflect.get(node, "constructor")
			: undefined;
		if (
			ctor !== null &&
			typeof ctor === "object" &&
			Object.hasOwn(ctor, "prototype")
		) {
			Reflect.deleteProperty(node, "constructor");
		}
		for (const child of Object.values(node)) {
			if (child !== null && typeof child === "object") pending.push(child);
		}
	}
	return parsed;
}
