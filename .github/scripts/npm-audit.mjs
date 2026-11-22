#!/usr/bin/env node
/**
 * The npm advisories gate: what a user of this package installs must carry no
 * high or critical advisory, and no advisory it cannot be updated out of.
 *
 * "What a user installs" is the package's dependencies, optional dependencies
 * and peers, resolved fresh from the registry the way `npm install` would in
 * an application: devDependencies are left out (nobody installs them), and so
 * is this repository's own lockfile and its `overrides` (they never reach a
 * user). A fresh resolution takes the newest version every range allows, so an
 * advisory still present afterwards is one the ranges cannot escape.
 *
 * It fails on:
 * - a high or critical advisory, fixable or not;
 * - an advisory of any severity whose fix the tree cannot reach (npm reports
 *   no fix, or only a semver-major change of a dependency we chose);
 * - an expired or malformed exception.
 *
 * An exception lives in `.github/npm-audit-exceptions.json`, committed:
 *   [{ "id": "GHSA-xxxx-xxxx-xxxx", "package": "name",
 *      "reason": "why it cannot be removed today and what lets it go",
 *      "expires": "YYYY-MM-DD" }]
 * An entry past its date fails the gate, so an exception is a dated claim
 * that has to be renewed on purpose, never a permanent silence.
 *
 * No dependency: node and the npm that ships with it.
 */

import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const root = resolve(process.argv[2] ?? ".");
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

function npm(args, cwd) {
	return spawnSync("npm", args, {
		cwd,
		encoding: "utf8",
		maxBuffer: 64 * 1024 * 1024,
		shell: process.platform === "win32",
	});
}

function productionTree(manifest) {
	const tree = {
		...(manifest.peerDependencies ?? {}),
		...(manifest.optionalDependencies ?? {}),
		...(manifest.dependencies ?? {}),
	};
	for (const [name, spec] of Object.entries(tree)) {
		if (/^(workspace|link|file|portal):/.test(spec)) {
			fail(`${name}@${spec}: a local specifier cannot be resolved the way a user resolves it`);
			delete tree[name];
		}
	}
	return tree;
}

function advisoryId(via) {
	const match = typeof via.url === "string" ? via.url.match(/GHSA(-[a-z0-9]{4}){3}/i) : null;
	return match ? match[0] : `npm:${via.source}`;
}

function audit(tree) {
	const probe = mkdtempSync(join(tmpdir(), "npm-audit-"));
	try {
		writeFileSync(
			join(probe, "package.json"),
			JSON.stringify({ name: "npm-audit-probe", private: true, dependencies: tree }, null, 2),
		);
		const base = ["install", "--package-lock-only", "--ignore-scripts", "--no-audit", "--no-fund"];
		let install = npm(base, probe);
		if (install.status !== 0 && /ERESOLVE/.test(install.stderr)) {
			// A peer conflict a user would also meet; resolve the way an
			// application that accepts it does, and say so.
			warn("peer conflict (ERESOLVE): resolved with --legacy-peer-deps");
			install = npm([...base, "--legacy-peer-deps"], probe);
		}
		if (install.status !== 0) {
			fail(`npm could not resolve the production tree:\n${install.stderr.trim()}`);
			return null;
		}
		const lock = readJson(join(probe, "package-lock.json"));
		const count = Object.keys(lock.packages ?? {}).filter((key) => key !== "").length;
		const result = npm(["audit", "--package-lock-only", "--json"], probe);
		let report;
		try {
			report = JSON.parse(result.stdout);
		} catch {
			fail(`npm audit gave no report:\n${result.stderr.trim()}`);
			return null;
		}
		if (report.error) {
			fail(`npm audit failed: ${report.error.summary ?? JSON.stringify(report.error)}`);
			return null;
		}
		return { report, count };
	} finally {
		rmSync(probe, { recursive: true, force: true });
	}
}

const manifestPath = join(root, "package.json");
if (!existsSync(manifestPath)) {
	console.log(`npm-audit: no package.json in ${root}; nothing here is resolved from npm by a user.`);
	process.exit(0);
}
const manifest = readJson(manifestPath);
const exceptions = loadExceptions();
const tree = productionTree(manifest);
const names = Object.keys(tree);

if (names.length === 0) {
	console.log(`npm-audit: ${manifest.name} has no dependencies or peers; nothing to audit.`);
} else {
	const audited = audit(tree);
	if (audited !== null) {
		const { report, count } = audited;
		let found = 0;
		for (const [name, entry] of Object.entries(report.vulnerabilities ?? {})) {
			const fix = entry.fixAvailable;
			const reachable = fix === true || (typeof fix === "object" && fix !== null && fix.isSemVerMajor !== true);
			for (const via of entry.via ?? []) {
				// A string names a dependency through which another entry's
				// advisory arrives; that entry reports it.
				if (typeof via !== "object" || via === null) continue;
				found++;
				const id = advisoryId(via);
				const line = `${id} ${via.severity} in ${name}@${via.range}: ${via.title} (${via.url})`;
				const exception = exceptions.find((e) => e.id === id && e.name === name);
				if (exception) {
					exception.used = true;
					warn(`excepted: ${line}`);
				} else if (BLOCKING.has(via.severity)) {
					fail(`${line}${reachable ? "" : " — no fix reachable"}`);
				} else if (!reachable) {
					fail(`${line} — no fix reachable from this package's ranges`);
				} else {
					warn(`fixable, below high: ${line}`);
				}
			}
		}
		console.log(
			`npm-audit: ${manifest.name}: ${names.length} direct (dependencies + peers), ${count} packages resolved, ${found} advisories.`,
		);
	}
}

for (const exception of exceptions) {
	if (!exception.used) warn(`exception ${exception.id} (${exception.name}) matched nothing: remove it`);
}
for (const message of warnings) console.log(`::warning::${message}`);
for (const message of failures) console.log(`::error::${message}`);
process.exit(failures.length === 0 ? 0 : 1);
