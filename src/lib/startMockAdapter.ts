// wotan-disable no-unused-expression
// tslint:disable:no-unused-expression

import { expect } from "chai";
import Module from "module";
import { fakeProcessExit, loadModuleInHarness } from "./harness/loader";
import { MockAdapter } from "./mocks/mockAdapter";
import { mockAdapterCore } from "./mocks/mockAdapterCore";
import { MockDatabase } from "./mocks/mockDatabase";

/**
 * Creates a module that is loaded instead of another one with the same name
 */
function createMockModule(id: string, mocks: Record<string, any>) {
	const ret = new Module(id);
	ret.exports = mocks;
	return ret;
}

export interface StartMockAdapterOptions {
	compact?: boolean;
	config?: Record<string, any>;
	instanceObjects?: ioBroker.Object[];
}

/**
 * Starts an adapter by executing its main file in a controlled offline environment.
 * The JS-Controller is replaced by mocks for the adapter and Objects and States DB, so
 * no working installation is necessary.
 * This method may throw (or reject) if something goes wrong during the adapter startup.
 * It returns an instance of the mocked adapter class and the database, so you can perform further tests.
 *
 * @param adapterMainFile The main file of the adapter to start. Must be a full path.
 */
export async function startMockAdapter(adapterMainFile: string, options: StartMockAdapterOptions = {}) {

	// Setup the mocks
	const databaseMock = new MockDatabase();
	// If instance objects are defined, populate the database mock with them
	if (options.instanceObjects && options.instanceObjects.length) {
		databaseMock.publishObjects(...options.instanceObjects);
	}
	let adapterMock: MockAdapter | undefined;
	const adapterCoreMock = mockAdapterCore(databaseMock, {
		onAdapterCreated: mock => {
			adapterMock = mock;
			// If an adapter configuration was given, set it on the mock
			if (options.config) mock.config = options.config;
		},
	});

	// Replace the following modules with mocks
	const mockedModules = {
		"@iobroker/adapter-core": createMockModule("@iobroker/adapter-core", adapterCoreMock),
	};
	// If the adapter supports compact mode and should be executed in "normal" mode,
	// we need to trick it into thinking it was not required
	const fakeNotRequired = !options.compact;
	// Make process.exit() test-safe
	const globalPatches = {
		process: { exit: fakeProcessExit },
	};

	// Load the adapter file into the test harness and capture it's module.exports
	const mainFileExport = loadModuleInHarness(adapterMainFile, {
		mockedModules,
		fakeNotRequired,
		globalPatches,
	});

	if (options.compact) {
		// In compact mode, the main file must export a function
		if (typeof mainFileExport !== "function") throw new Error("The adapter's main file must export a function in compact mode!");
		// Call it to initialize the adapter
		mainFileExport();
	}

	// Assert some basic stuff
	if (adapterMock == undefined) throw new Error("The adapter was not initialized!");
	expect(adapterMock.readyHandler).to.exist;

	// Execute the ready method (synchronously or asynchronously)
	let processExitCode: number | undefined;
	let terminateReason: string | undefined;
	try {
		const readyResult = adapterMock.readyHandler!() as undefined | Promise<void>;
		if (readyResult instanceof Promise) await readyResult;
	} catch (e) {
		if (e instanceof Error) {
			const anyError = e as any;
			if (typeof anyError.processExitCode === "number") {
				processExitCode = anyError.processExitCode;
			} else if (typeof anyError.terminateReason === "string") {
				terminateReason = anyError.terminateReason;
			} else {
				// This error was not meant for us, pass it through
				throw e;
			}
		}
	}

	// Return the mock instances so the tests can work with them
	return {
		databaseMock,
		adapterMock,
		processExitCode,
		terminateReason,
	};
}
