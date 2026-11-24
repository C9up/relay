#!/usr/bin/env node
/**
 * The npm advisories gate, in two passes.
 *
 * `node npm-audit.mjs` — what a user of this package installs must carry no
 * high or critical advisory, and no advisory it cannot be updated out of.
 * "What a user installs" is the package's dependencies, optional dependencies
 * and peers, resolved fresh from the registry the way `npm install` would in
 * an application: devDependencies are left out (nobody installs them), and so
 * is this repository's own lockfile and its `overrides` (they never reach a
 * user). A fresh resolution takes the newest version every range allows, so an
 * advisory still present afterwards is one the ranges cannot escape.
 *
 * `node npm-audit.mjs --installed` — what this CI run installed, run right
 * after `pnpm install`: the lockfile that install wrote, devDependencies
 * included, audited as installed (`pnpm audit`). The test toolchain runs here
 * with this repository's secrets and, in the publish job, its npm token, so a
 * high or critical advisory in it fails the run like one in what ships. It
 * reads the real install rather than resolving again: the CI install keeps
 * the workspace's seven-day release age, so it can sit on an older version
 * than a fresh resolution would pick.
 *
 * Both passes fail on:
 * - a high or critical advisory, fixable or not;
 * - an advisory of any severity whose fix cannot be reached (the user pass:
 *   npm reports no fix, or only a semver-major change of a dependency we
 *   chose; the installed pass: the advisory names no patched version);
 * - an expired or malformed exception.
 *
 * An exception lives in `.github/npm-audit-exceptions.json`, committed, and
 * serves both passes:
 *   [{ "id": "GHSA-xxxx-xxxx-xxxx", "package": "name",
 *      "reason": "why it cannot be removed today and what lets it go",
 *      "expires": "YYYY-MM-DD" }]
 * An entry past its date fails the gate, so an exception is a dated claim
 * that has to be renewed on purpose, never a permanent silence.
 *
 * No dependency: node, the npm that ships with it, and (for --installed) the
 * pnpm that just installed.
 */

import { spawnSync } from "node:child_process";
import {
	existsSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const args = process.argv.slice(2);
const installed = args.includes("--installed");
const root = resolve(args.find((arg) => !arg.startsWith("--")) ?? ".");
const BLOCKING = new Set(["high", "critical"]);
const GHSA = /^GHSA(-[23456789cfghjmpqrvwx]{4}){3}$/;

const failures = [];
const warnings = [];
const fail = (message) => failures.push(message);
const warn = (message) => warnings.push(message);

function readJson(path) {
	return JSON.parse(readFileSync(path, "utf8"));
}

function loadExceptions() {
	const path = join(root, ".github", "npm-audit-exceptions.json");
	if (!existsSync(path)) return [];
	const entries = readJson(path);
	if (!Array.isArray(entries)) {
		fail(`${path}: expected an array of exceptions`);
		return [];
	}
	const today = new Date().toISOString().slice(0, 10);
	const valid = [];
	for (const [index, entry] of entries.entries()) {
		const where = `${path} [${index}]`;
		const { id, package: name, reason, expires } = entry ?? {};
		if (typeof id !== "string" || !GHSA.test(id)) {
			fail(`${where}: "id" must be a GHSA identifier`);
			continue;
		}
		if (typeof name !== "string" || name === "") {
			fail(`${where}: "package" names the vulnerable package`);
			continue;
		}
		if (typeof reason !== "string" || reason.trim().length < 20) {
			fail(`${where}: "reason" must say why it stays and what lets it go`);
			continue;
		}
		if (typeof expires !== "string" || !/^\d{4}-\d{2}-\d{2}$/.test(expires)) {
			fail(`${where}: "expires" must be a YYYY-MM-DD date`);
			continue;
		}
		if (expires < today) {
			fail(`${where}: the exception for ${id} (${name}) expired on ${expires}`);
			continue;
		}
		valid.push({ id, name, used: false });
	}
	return valid;
}

function run(command, commandArgs, cwd) {
	return spawnSync(command, commandArgs, {
		cwd,
		encoding: "utf8",
		maxBuffer: 64 * 1024 * 1024,
		shell: process.platform === "win32",
	});
}

/**
 * The package's own platform packages (`npm/<suffix>/package.json`): binaries
 * with no dependency of their own, published by this repository's publish job
 * right before the package. Resolving them here would ask npm for a version
 * that job has not published yet.
 */
function ownPlatformPackages() {
	const dir = join(root, "npm");
	if (!existsSync(dir)) return new Set();
	const names = new Set();
	for (const suffix of readdirSync(dir)) {
		const path = join(dir, suffix, "package.json");
		if (existsSync(path)) names.add(readJson(path).name);
	}
	return names;
}

function productionTree(manifest) {
	const tree = {
		...(manifest.peerDependencies ?? {}),
		...(manifest.optionalDependencies ?? {}),
		...(manifest.dependencies ?? {}),
	};
	for (const name of ownPlatformPackages()) delete tree[name];
	for (const [name, spec] of Object.entries(tree)) {
		if (/^(workspace|link|file|portal):/.test(spec)) {
			fail(
				`${name}@${spec}: a local specifier cannot be resolved the way a user resolves it`,
			);
			delete tree[name];
		}
	}
	return tree;
}

function advisoryId(url, fallback) {
	const match =
		typeof url === "string" ? url.match(/GHSA(-[a-z0-9]{4}){3}/i) : null;
	return match ? match[0] : fallback;
}

/** Judge one advisory against the exceptions and the blocking rules. */
function judge(
	exceptions,
	{ id, severity, name, range, title, url, reachable },
) {
	const line = `${id} ${severity} in ${name}@${range}: ${title} (${url})`;
	const exception = exceptions.find((e) => e.id === id && e.name === name);
	if (exception) {
		exception.used = true;
		warn(`excepted: ${line}`);
	} else if (BLOCKING.has(severity)) {
		fail(`${line}${reachable ? "" : " — no fix reachable"}`);
	} else if (!reachable) {
		fail(`${line} — no fix reachable`);
	} else {
		warn(`fixable, below high: ${line}`);
	}
}

function auditUserTree(manifest, exceptions) {
	const tree = productionTree(manifest);
	const names = Object.keys(tree);
	if (names.length === 0) {
		console.log(
			`npm-audit: ${manifest.name} has no dependencies or peers; nothing to audit.`,
		);
		return;
	}
	const probe = mkdtempSync(join(tmpdir(), "npm-audit-"));
	try {
		writeFileSync(
			join(probe, "package.json"),
			JSON.stringify(
				{ name: "npm-audit-probe", private: true, dependencies: tree },
				null,
				2,
			),
		);
		const base = [
			"install",
			"--package-lock-only",
			"--ignore-scripts",
			"--no-audit",
			"--no-fund",
		];
		let install = run("npm", base, probe);
		if (install.status !== 0 && /ERESOLVE/.test(install.stderr)) {
			// A peer conflict a user would also meet; resolve the way an
			// application that accepts it does, and say so.
			warn("peer conflict (ERESOLVE): resolved with --legacy-peer-deps");
			install = run("npm", [...base, "--legacy-peer-deps"], probe);
		}
		if (install.status !== 0) {
			fail(
				`npm could not resolve the production tree:\n${install.stderr.trim()}`,
			);
			return;
		}
		const lock = readJson(join(probe, "package-lock.json"));
		const count = Object.keys(lock.packages ?? {}).filter(
			(key) => key !== "",
		).length;
		const result = run(
			"npm",
			["audit", "--package-lock-only", "--json"],
			probe,
		);
		let report;
		try {
			report = JSON.parse(result.stdout);
		} catch {
			fail(`npm audit gave no report:\n${result.stderr.trim()}`);
			return;
		}
		if (report.error) {
			fail(
				`npm audit failed: ${report.error.summary ?? JSON.stringify(report.error)}`,
			);
			return;
		}
		let found = 0;
		for (const [name, entry] of Object.entries(report.vulnerabilities ?? {})) {
			const fix = entry.fixAvailable;
			const reachable =
				fix === true ||
				(typeof fix === "object" && fix !== null && fix.isSemVerMajor !== true);
			for (const via of entry.via ?? []) {
				// A string names a dependency through which another entry's
				// advisory arrives; that entry reports it.
				if (typeof via !== "object" || via === null) continue;
				found++;
				judge(exceptions, {
					id: advisoryId(via.url, `npm:${via.source}`),
					severity: via.severity,
					name,
					range: via.range,
					title: via.title,
					url: via.url,
					reachable,
				});
			}
		}
		console.log(
			`npm-audit: ${manifest.name}: ${names.length} direct (dependencies + peers), ${count} packages resolved, ${found} advisories.`,
		);
	} finally {
		rmSync(probe, { recursive: true, force: true });
	}
}

function auditInstalledTree(manifest, exceptions) {
	if (!existsSync(join(root, "pnpm-lock.yaml"))) {
		fail(
			"--installed runs after `pnpm install`: there is no pnpm-lock.yaml here to audit",
		);
		return;
	}
	const result = run("pnpm", ["audit", "--json"], root);
	let report;
	try {
		report = JSON.parse(result.stdout);
	} catch {
		fail(
			`pnpm audit gave no report:\n${(result.stderr || result.stdout).trim()}`,
		);
		return;
	}
	if (report.error) {
		fail(
			`pnpm audit failed: ${report.error.summary ?? report.error.message ?? JSON.stringify(report.error)}`,
		);
		return;
	}
	let found = 0;
	for (const advisory of Object.values(report.advisories ?? {})) {
		found++;
		const versions = [
			...new Set((advisory.findings ?? []).map((f) => f.version)),
		].join(", ");
		judge(exceptions, {
			id:
				advisory.github_advisory_id ??
				advisoryId(advisory.url, `npm:${advisory.id}`),
			severity: advisory.severity,
			name: advisory.module_name,
			range: versions || advisory.vulnerable_versions,
			title: advisory.title,
			url: advisory.url,
			// The registry writes "<0.0.0" for an advisory nothing patches.
			reachable:
				typeof advisory.patched_versions === "string" &&
				advisory.patched_versions !== "<0.0.0",
		});
	}
	const total = report.metadata?.totalDependencies ?? 0;
	const development = report.metadata?.devDependencies ?? 0;
	console.log(
		`npm-audit --installed: ${manifest.name}: ${total} installed packages (${development} reached through devDependencies), ${found} advisories.`,
	);
}

const manifestPath = join(root, "package.json");
if (!existsSync(manifestPath)) {
	console.log(
		`npm-audit: no package.json in ${root}; nothing here is resolved from npm.`,
	);
	process.exit(0);
}
const manifest = readJson(manifestPath);
const exceptions = loadExceptions();
if (installed) auditInstalledTree(manifest, exceptions);
else auditUserTree(manifest, exceptions);

for (const exception of exceptions) {
	// Each pass sees only its own tree, so an exception for the other one
	// matches nothing here; say so without failing.
	if (!exception.used)
		warn(
			`exception ${exception.id} (${exception.name}) matched nothing in this pass`,
		);
}
for (const message of warnings) console.log(`::warning::${message}`);
for (const message of failures) console.log(`::error::${message}`);
process.exit(failures.length === 0 ? 0 : 1);
