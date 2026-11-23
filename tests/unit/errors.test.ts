import { Exception } from "@c9up/flare";
import { describe, expect, it } from "vitest";
import { errors } from "../../src/index.js";

describe("relay > errors", () => {
	it("are exported as one namespace of flare exceptions", () => {
		const error = new errors.E_RELAY_MAX_PENDING();
		expect(error).toBeInstanceOf(Exception);
		expect(error).toMatchObject({ code: "E_RELAY_MAX_PENDING", status: 503 });
		expect(new errors.E_RELAY_HUB_PATH_TAKEN(["/hubs/chat"])).toMatchObject({
			code: "E_RELAY_HUB_PATH_TAKEN",
			status: 500,
			message:
				'A hub is already mounted at "/hubs/chat" — unmount it or pick another path.',
		});
	});
});
