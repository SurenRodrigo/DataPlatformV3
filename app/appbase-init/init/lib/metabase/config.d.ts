export interface Dashboard {
    readonly name: string;
    readonly template: string;
}
export declare function loadDashboards(): Promise<ReadonlyArray<Dashboard>>;
export declare function loadDashboard(name: string): Promise<Dashboard>;
