import { defineConfig } from "vitest/config";

export default defineConfig({
	test: {
		coverage: {
			provider: "v8",
			include: ["src/**"],
			// `src/vendor/**` is generated from scripts/vendor/ and identical in every
			// package that carries it, so measuring it here counts the same lines N
			// times and holds this package to a floor for code it cannot change. The
			// behaviour is pinned where it broke: bay's quasar-bridge suite covers the
			// two manager shapes the loader has to accept.
			exclude: ["src/**/*.d.ts", "src/vendor/**"],
			reporter: ["text-summary", "json-summary"],
			thresholds: {
				lines: 79,
				statements: 76,
				branches: 66,
				functions: 75,
			},
		},
	},
});
