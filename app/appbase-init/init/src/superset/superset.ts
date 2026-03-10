import axios, {AxiosInstance} from 'axios';
import {VError} from 'verror';

export function wrapApiError(cause: unknown, msg: string): Error {
  // Omit verbose axios error
  const truncated = new VError((cause as Error).message);
  return new VError(truncated, msg);
}

export interface SupersetConfig {
  readonly url: string;
  readonly username: string;
  readonly password: string;
}

interface DatabaseConfig {
  readonly database_name: string;
  readonly sqlalchemy_uri: string;
  readonly engine: string;
  readonly configuration_method?: string;
  readonly expose_in_sqllab?: boolean;
  readonly allow_run_async?: boolean;
  readonly allow_ctas?: boolean;
  readonly allow_cvas?: boolean;
  readonly allow_dml?: boolean;
  readonly allow_file_upload?: boolean;
}

export class Superset {
  constructor(
    private readonly api: AxiosInstance,
    private readonly refreshToken: string
  ) {}

  static async fromConfig(cfg: SupersetConfig): Promise<Superset> {
    const tokens = await Superset.authenticate(cfg);
    const api = axios.create({
      baseURL: `${cfg.url}/api/v1`,
      headers: {
        'Authorization': `Bearer ${tokens.access_token}`,
        'Content-Type': 'application/json',
      },
    });
    return new Superset(api, tokens.refresh_token);
  }

  private static async authenticate(cfg: SupersetConfig): Promise<{access_token: string, refresh_token: string}> {
    const {url, username, password} = cfg;
    try {
      const {data} = await axios.post(`${url}/api/v1/security/login`, {
        username,
        password,
        provider: 'db',
        refresh: true
      });
      return {
        access_token: data.access_token,
        refresh_token: data.refresh_token
      };
    } catch (err) {
      throw wrapApiError(err, 'failed to authenticate with Superset');
    }
  }

  async refreshAccessToken(): Promise<void> {
    try {
      const {data} = await axios.post(`${this.api.defaults.baseURL?.replace('/api/v1', '')}/api/v1/security/refresh`, {
        refresh_token: this.refreshToken
      });
      this.api.defaults.headers['Authorization'] = `Bearer ${data.access_token}`;
    } catch (err) {
      throw wrapApiError(err, 'failed to refresh access token');
    }
  }

  async getDatabases(): Promise<any[]> {
    try {
      const {data} = await this.api.get('database/');
      return data.result || [];
    } catch (err) {
      throw wrapApiError(err, 'unable to get databases');
    }
  }

  async getDatabase(name: string): Promise<any | undefined> {
    try {
      const databases = await this.getDatabases();
      return databases.find((db: any) => db?.database_name === name);
    } catch (err) {
      throw wrapApiError(err, 'unable to find database: ' + name);
    }
  }

  async createDatabase(config: DatabaseConfig): Promise<any> {
    try {
      const {data} = await this.api.post('database/', config);
      return data;
    } catch (err) {
      throw wrapApiError(err, `failed to create database: ${config.database_name}`);
    }
  }

  async updateDatabase(id: number, updates: Partial<DatabaseConfig>): Promise<any> {
    try {
      const {data} = await this.api.put(`database/${id}`, updates);
      return data;
    } catch (err) {
      throw wrapApiError(err, `failed to update database: ${id}`);
    }
  }

  async testDatabaseConnection(config: DatabaseConfig): Promise<boolean> {
    try {
      const {data} = await this.api.post('database/test_connection', config);
      return data.message === 'OK';
    } catch (err) {
      throw wrapApiError(err, 'failed to test database connection');
    }
  }

  async syncDatabase(databaseId: number): Promise<void> {
    try {
      // Get all datasets for this database and sync each one
      const datasets = await this.getDatasets();
      const databaseDatasets = datasets.filter((dataset: any) => dataset.database?.id === databaseId);
      
      const syncPromises = databaseDatasets.map((dataset: any) => 
        this.syncDataset(dataset.id)
      );
      
      await Promise.all(syncPromises);
    } catch (err) {
      throw wrapApiError(err, `failed to sync database ${databaseId}`);
    }
  }

  async syncDataset(datasetId: number): Promise<void> {
    try {
      await this.api.put(`dataset/${datasetId}/refresh`);
    } catch (err) {
      throw wrapApiError(err, `failed to sync dataset ${datasetId}`);
    }
  }

  async getDatasetByName(databaseName: string, tableName?: string): Promise<any | undefined> {
    try {
      const datasets = await this.getDatasets();
      return datasets.find((dataset: any) => {
        if (tableName) {
          return dataset.database?.database_name === databaseName && 
                 dataset.table_name === tableName;
        }
        return dataset.database?.database_name === databaseName;
      });
    } catch (err) {
      throw wrapApiError(err, `unable to find dataset for database: ${databaseName}`);
    }
  }

  async getTables(databaseId: number): Promise<any[]> {
    try {
      const {data} = await this.api.get(`database/${databaseId}/tables/`);
      return data.result || [];
    } catch (err) {
      throw wrapApiError(err, `unable to get tables for database ${databaseId}`);
    }
  }

  async getDashboards(): Promise<any[]> {
    try {
      const {data} = await this.api.get('dashboard/');
      return data.result || [];
    } catch (err) {
      throw wrapApiError(err, 'unable to get dashboards');
    }
  }

  async getDashboard(id: number): Promise<any> {
    try {
      const {data} = await this.api.get(`dashboard/${id}`);
      return data.result;
    } catch (err) {
      throw wrapApiError(err, 'unable to get dashboard: ' + id);
    }
  }

  async createDashboard(dashboard: any): Promise<any> {
    try {
      const {data} = await this.api.post('dashboard/', dashboard);
      return data;
    } catch (err) {
      throw wrapApiError(err, 'unable to create dashboard');
    }
  }

  async updateDashboard(id: number, dashboard: any): Promise<any> {
    try {
      const {data} = await this.api.put(`dashboard/${id}`, dashboard);
      return data;
    } catch (err) {
      throw wrapApiError(err, `unable to update dashboard: ${id}`);
    }
  }

  async getCharts(): Promise<any[]> {
    try {
      const {data} = await this.api.get('chart/');
      return data.result || [];
    } catch (err) {
      throw wrapApiError(err, 'unable to get charts');
    }
  }

  async getChart(id: number): Promise<any> {
    try {
      const {data} = await this.api.get(`chart/${id}`);
      return data.result;
    } catch (err) {
      throw wrapApiError(err, 'unable to get chart: ' + id);
    }
  }

  async createChart(chart: any): Promise<any> {
    try {
      const {data} = await this.api.post('chart/', chart);
      return data;
    } catch (err) {
      throw wrapApiError(err, 'unable to create chart');
    }
  }

  async getDatasets(): Promise<any[]> {
    try {
      const {data} = await this.api.get('dataset/');
      return data.result || [];
    } catch (err) {
      throw wrapApiError(err, 'unable to get datasets');
    }
  }

  async createDataset(dataset: any): Promise<any> {
    try {
      const {data} = await this.api.post('dataset/', dataset);
      return data;
    } catch (err) {
      throw wrapApiError(err, 'unable to create dataset');
    }
  }

  async importDashboard(dashboardData: any): Promise<any> {
    try {
      const {data} = await this.api.post('dashboard/import/', dashboardData);
      return data;
    } catch (err) {
      throw wrapApiError(err, 'unable to import dashboard');
    }
  }

  async exportDashboard(id: number): Promise<any> {
    try {
      const {data} = await this.api.get(`dashboard/export/?q=!(${id})`);
      return data;
    } catch (err) {
      throw wrapApiError(err, `unable to export dashboard: ${id}`);
    }
  }

  async healthCheck(): Promise<boolean> {
    try {
      const {data} = await this.api.get('/health');
      return data.status === 'ok';
    } catch (err) {
      return false;
    }
  }
} 