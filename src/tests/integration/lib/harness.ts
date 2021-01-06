/* eslint-disable @typescript-eslint/camelcase */
/* eslint-disable @typescript-eslint/no-var-requires */
import {
	Client as ObjectsClient,
	Server as ObjectsServer,
} from "@iobroker/db-objects-file";
import {
	Client as StatesClient,
	Server as StatesServer,
} from "@iobroker/db-states-file";
import { wait } from "alcalzone-shared/async";
import { extend } from "alcalzone-shared/objects";
import { ChildProcess, spawn } from "child_process";
import debugModule from "debug";
import { EventEmitter } from "events";
import * as path from "path";
import {
	getAdapterName,
	getAppName,
	locateAdapterMainFile,
} from "../../../lib/adapterTools";
import { DBConnection } from "./dbConnection";
import { getTestAdapterDir, getTestControllerDir } from "./tools";

const debug = debugModule("testing:integration:TestHarness");

const isWindows = /^win/.test(process.platform);

/** The logger instance for the objects and states DB */
const logger = {
	silly: console.log,
	debug: console.log,
	info: console.log,
	warn: console.warn,
	error: console.error,
};

export interface TestHarness {
	on(event: "objectChange", handler: ioBroker.ObjectChangeHandler): this;
	on(event: "stateChange", handler: ioBroker.StateChangeHandler): this;
	on(event: "failed", handler: (codeOrSignal: number | string) => void): this;
}

const fromAdapterID = "system.adapter.test.0";

/**
 * The test harness capsules the execution of the JS-Controller and the adapter instance and monitors their status.
 * Use it in every test to start a fresh adapter instance
 */
export class TestHarness extends EventEmitter {
	/**
	 * @param adapterDir The root directory of the adapter
	 * @param testDir The directory the integration tests are executed in
	 */
	public constructor(private adapterDir: string, private testDir: string) {
		super();

		debug("Creating instance");
		this.adapterName = getAdapterName(this.adapterDir);
		this.appName = getAppName(adapterDir);

		this.testControllerDir = getTestControllerDir(this.appName, testDir);
		this.testAdapterDir = getTestAdapterDir(this.adapterDir, testDir);

		debug(`  directories:`);
		debug(`    controller: ${this.testControllerDir}`);
		debug(`    adapter:    ${this.testAdapterDir}`);
		debug(`  appName:           ${this.appName}`);
		debug(`  adapterName:       ${this.adapterName}`);

		this.dbConnection = new DBConnection(this.appName, this.testDir);
	}

	private adapterName: string;
	private appName: string;
	private testControllerDir: string;
	private testAdapterDir: string;
	private dbConnection: DBConnection;

	private _objectsServer: any;
	private _objectsClient: any;
	private _statesServer: any;
	private _statesClient: any;

	private _adapterProcess: ChildProcess | undefined;
	/** The process the adapter is running in */
	public get adapterProcess(): ChildProcess | undefined {
		return this._adapterProcess;
	}

	private _adapterExit: number | string | undefined;
	/** Contains the adapter exit code or signal if it was terminated unexpectedly */
	public get adapterExit(): number | string | undefined {
		return this._adapterExit;
	}

	/** Creates the objects DB and sets up listeners for it */
	private async createObjectsDB(): Promise<void> {
		debug("creating objects DB");

		const settings = {
			connection: {
				type: "file",
				host: "127.0.0.1",
				port: 19001,
				user: "",
				pass: "",
				noFileCache: false,
				connectTimeout: 2000,
			},
			logger,
		};

		// First create the server
		await new Promise<void>((resolve) => {
			this._objectsServer = new ObjectsServer({
				...settings,
				connected: () => {
					resolve();
				},
			});
		});

		// Then the client
		await new Promise<void>((resolve) => {
			this._objectsClient = new ObjectsClient({
				...settings,
				connected: () => {
					this._objectsClient.subscribe("*");
					resolve();
				},
				change: this.emit.bind(this, "objectChange"),
			});
		});

		debug("  => done!");
	}

	/** Creates the states DB and sets up listeners for it */
	private async createStatesDB(): Promise<void> {
		debug("creating states DB");

		const settings = {
			connection: {
				type: "file",
				host: "127.0.0.1",
				port: 19000,
				options: {
					auth_pass: null,
					retry_max_delay: 15000,
				},
			},
			logger,
		};

		// First create the server
		await new Promise<void>((resolve) => {
			this._statesServer = new StatesServer({
				...settings,
				connected: () => {
					resolve();
				},
			});
		});

		// Then the client
		await new Promise<void>((resolve) => {
			this._statesClient = new StatesClient({
				...settings,
				connected: () => {
					this._statesClient.subscribe("*");
					resolve();
				},
				change: this.emit.bind(this, "stateChange"),
			});
		});

		debug("  => done!");
	}

	/** Checks if the controller instance is running */
	public isControllerRunning(): boolean {
		return (
			!!this._objectsServer ||
			!!this._objectsClient ||
			!!this._statesServer ||
			!!this._statesClient
		);
	}

	/** Starts the controller instance by creating the databases */
	public async startController(): Promise<void> {
		debug("starting controller instance...");
		if (this.isControllerRunning())
			throw new Error("The Controller is already running!");
		await this.createObjectsDB();
		await this.createStatesDB();
		debug("controller instance created");
	}

	/** Stops the controller instance (and the adapter if it is running) */
	public async stopController(): Promise<void> {
		if (!this.isControllerRunning()) return;

		if (!this.didAdapterStop()) {
			debug("Stopping adapter instance...");
			// Give the adapter time to stop (as long as configured in the io-package.json)
			let stopTimeout: number;
			try {
				stopTimeout = (
					await this._objectsClient.getObjectAsync(
						`system.adapter.${this.adapterName}.0`,
					)
				).common.stopTimeout;
				stopTimeout += 1000;
			} catch {}
			stopTimeout ||= 5000; // default 5s
			debug(`  => giving it ${stopTimeout}ms to terminate`);
			await Promise.race([this.stopAdapter(), wait(stopTimeout)]);

			if (this.isAdapterRunning()) {
				debug("Adapter did not terminate, killing it");
				this._adapterProcess!.kill("SIGKILL");
			} else {
				debug("Adapter terminated");
			}
		} else {
			debug("Adapter failed to start - no need to terminate!");
		}

		debug("Stopping controller instance...");
		// Stop clients before servers
		await this._objectsClient?.destroy();
		await this._objectsServer?.destroy();
		await this._statesClient?.destroy();
		await this._statesServer?.destroy();

		this._objectsClient = null;
		this._objectsServer = null;
		this._statesClient = null;
		this._statesServer = null;

		debug("Controller instance stopped");
	}

	/**
	 * Starts the adapter in a separate process and monitors its status
	 * @param env Additional environment variables to set
	 */
	public async startAdapter(env: NodeJS.ProcessEnv = {}): Promise<void> {
		if (this.isAdapterRunning())
			throw new Error("The adapter is already running!");
		else if (this.didAdapterStop())
			throw new Error(
				"This test harness has already been used. Please create a new one for each test!",
			);

		const mainFileAbsolute = await locateAdapterMainFile(
			this.testAdapterDir,
		);
		const mainFileRelative = path.relative(
			this.testAdapterDir,
			mainFileAbsolute,
		);

		const onClose = (code: number | undefined, signal: string): void => {
			this._adapterProcess!.removeAllListeners();
			this._adapterExit = code != undefined ? code : signal;
			this.emit("failed", this._adapterExit);
		};

		this._adapterProcess = spawn(
			isWindows ? "node.exe" : "node",
			[mainFileRelative, "--console"],
			{
				cwd: this.testAdapterDir,
				stdio: ["inherit", "inherit", "inherit"],
				env: { ...process.env, ...env },
			},
		)
			.on("close", onClose)
			.on("exit", onClose);
	}

	/**
	 * Starts the adapter in a separate process and resolves after it has started
	 * @param env Additional environment variables to set
	 */
	public async startAdapterAndWait(
		env: NodeJS.ProcessEnv = {},
	): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			this.on("stateChange", async (id, state) => {
				if (
					id === `system.adapter.${this.adapterName}.0.alive` &&
					state &&
					state.val === true
				) {
					resolve();
				}
			})
				.on("failed", (code) => {
					reject(
						new Error(
							`The adapter startup was interrupted unexpectedly with ${
								typeof code === "number" ? "code" : "signal"
							} ${code}`,
						),
					);
				})
				.startAdapter(env);
		});
	}

	/** Tests if the adapter process is still running */
	public isAdapterRunning(): boolean {
		return !!this._adapterProcess;
	}

	/** Tests if the adapter process has already exited */
	public didAdapterStop(): boolean {
		return this._adapterExit != undefined;
	}

	/** Stops the adapter process */
	public stopAdapter(): Promise<void> | undefined {
		if (!this.isAdapterRunning()) return;

		return new Promise<void>(async (resolve) => {
			const onClose = (
				code: number | undefined,
				signal: string,
			): void => {
				if (!this._adapterProcess) return;
				this._adapterProcess.removeAllListeners();

				this._adapterExit = code != undefined ? code : signal;
				this._adapterProcess = undefined;
				debug("Adapter process terminated:");
				debug(`  Code:   ${code}`);
				debug(`  Signal: ${signal}`);
				resolve();
			};

			this._adapterProcess!.removeAllListeners()
				.on("close", onClose)
				.on("exit", onClose);

			// Tell adapter to stop
			if (this._statesClient) {
				await this._statesClient.setStateAsync(
					`system.adapter.${this.adapterName}.0.sigKill`,
					{
						val: -1,
						from: "system.host.testing",
					},
				);
			} else {
				this._adapterProcess?.kill("SIGTERM");
			}
		});
	}

	/**
	 * Updates the adapter config. The changes can be a subset of the target object
	 */
	public async changeAdapterConfig(
		appName: string,
		testDir: string,
		adapterName: string,
		changes: any,
	): Promise<void> {
		const objects = await this.dbConnection.readObjectsDB();
		const adapterInstanceId = `system.adapter.${adapterName}.0`;
		if (objects && adapterInstanceId in objects) {
			const target = objects[adapterInstanceId];
			extend(target, changes);
			await this.dbConnection.writeObjectsDB(objects);
		}
	}

	/** Enables the sendTo method */
	public enableSendTo(): Promise<void> {
		return new Promise<void>((resolve) => {
			this._objectsClient.setObject(
				fromAdapterID,
				{
					common: {},
					type: "instance",
				},
				() => {
					this._statesClient.subscribeMessage(fromAdapterID);
					resolve();
				},
			);
		});
	}

	private sendToID = 1;

	/** Sends a message to an adapter instance */
	public sendTo(
		target: string,
		command: string,
		message: any,
		callback: ioBroker.MessageCallback,
	): void {
		const stateChangedHandler: ioBroker.StateChangeHandler = (
			id,
			state,
		) => {
			if (id === `messagebox.${fromAdapterID}`) {
				callback((state as any).message);
				this.removeListener("stateChange", stateChangedHandler);
			}
		};
		this.addListener("stateChange", stateChangedHandler);

		this._statesClient.pushMessage(
			`system.adapter.${target}`,
			{
				command: command,
				message: message,
				from: fromAdapterID,
				callback: {
					message: message,
					id: this.sendToID++,
					ack: false,
					time: Date.now(),
				},
			},
			(err: any, id: any) => console.log("published message " + id),
		);
	}
}
