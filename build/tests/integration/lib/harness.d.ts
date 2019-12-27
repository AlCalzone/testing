/// <reference types="iobroker" />
/// <reference types="node" />
import { ChildProcess } from "child_process";
import { EventEmitter } from "events";
export interface TestHarness {
    on(event: "objectChange", handler: ioBroker.ObjectChangeHandler): this;
    on(event: "stateChange", handler: ioBroker.StateChangeHandler): this;
    on(event: "failed", handler: (codeOrSignal: number | string) => void): this;
}
/**
 * The test harness capsules the execution of the JS-Controller and the adapter instance and monitors their status.
 * Use it in every test to start a fresh adapter instance
 */
export declare class TestHarness extends EventEmitter {
    private adapterDir;
    private testDir;
    /**
     * @param adapterDir The root directory of the adapter
     * @param testDir The directory the integration tests are executed in
     */
    constructor(adapterDir: string, testDir: string);
    private adapterName;
    private appName;
    private testControllerDir;
    private testAdapterDir;
    private dbConnection;
    private _objects;
    /** The actual objects DB */
    get objects(): any;
    private _states;
    /** The actual states DB */
    get states(): any;
    private _adapterProcess;
    /** The process the adapter is running in */
    get adapterProcess(): ChildProcess | undefined;
    private _adapterExit;
    /** Contains the adapter exit code or signal if it was terminated unexpectedly */
    get adapterExit(): number | string | undefined;
    /** Creates the objects DB and sets up listeners for it */
    private createObjectsDB;
    /** Creates the states DB and sets up listeners for it */
    private createStatesDB;
    /** Checks if the controller instance is running */
    isControllerRunning(): boolean;
    /** Starts the controller instance by creating the databases */
    startController(): Promise<void>;
    /** Stops the controller instance (and the adapter if it is running) */
    stopController(): Promise<void>;
    /**
     * Starts the adapter in a separate process and monitors its status
     * @param env Additional environment variables to set
     */
    startAdapter(env?: NodeJS.ProcessEnv): Promise<void>;
    /**
     * Starts the adapter in a separate process and resolves after it has started
     * @param env Additional environment variables to set
     */
    startAdapterAndWait(env?: NodeJS.ProcessEnv): Promise<void>;
    /** Tests if the adapter process is still running */
    isAdapterRunning(): boolean;
    /** Tests if the adapter process has already exited */
    didAdapterStop(): boolean;
    /** Stops the adapter process */
    stopAdapter(): Promise<void> | undefined;
    /**
     * Updates the adapter config. The changes can be a subset of the target object
     */
    changeAdapterConfig(appName: string, testDir: string, adapterName: string, changes: any): Promise<void>;
    /** Enables the sendTo method */
    enableSendTo(): Promise<void>;
    private sendToID;
    /** Sends a message to an adapter instance */
    sendTo(target: string, command: string, message: any, callback: ioBroker.MessageCallback): void;
}
