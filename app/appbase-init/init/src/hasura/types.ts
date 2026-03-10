interface Table {
  schema: string;
  name: string;
}

export interface ObjectRelationship {
  name: string;
  using: {
    foreign_key_constraint_on: string;
  };
}

export interface ArrayRelationship {
  name: string;
  using: {
    foreign_key_constraint_on: {
      column: string;
      table: Table;
    };
  };
}

interface TableWithRelationships {
  table: Table;
  object_relationships: ReadonlyArray<ObjectRelationship>;
  array_relationships: ReadonlyArray<ArrayRelationship>;
}

export interface Source {
  name: string;
  kind: string;
  tables: ReadonlyArray<TableWithRelationships>;
  configuration: any;
}

export interface ForeignKey {
  childTable: string;
  parentTable: string;
  column: string;
  relationshipNames: {
    object: string;
    array: string;
  };
}

export interface TableRelationships {
  objectRels: ReadonlyArray<ObjectRelationship>;
  arrayRels: ReadonlyArray<ArrayRelationship>;
}

export interface Query {
  name: string;
  query: string;
}

export interface QueryCollection {
  name: string;
  definition: {
    queries: ReadonlyArray<Query>;
  };
}

export interface Endpoint {
  name: string;
  url: string;
  comment: string | null;
  methods: ReadonlyArray<string>;
  definition: {
    query: {
      query_name: string;
      collection_name: string;
    };
  };
}

// Permission management types
export interface PermissionRule {
  [key: string]: any;
}

export interface SelectPermission {
  columns: string[] | '*';
  filter: PermissionRule;
  limit?: number;
  allow_aggregations?: boolean;
}

export interface InsertPermission {
  columns: string[] | '*';
  check: PermissionRule;
}

export interface UpdatePermission {
  columns: string[] | '*';
  filter: PermissionRule;
  check?: PermissionRule;
}

export interface DeletePermission {
  filter: PermissionRule;
}

export interface Permission {
  role: string;
  permission: SelectPermission | InsertPermission | UpdatePermission | DeletePermission;
}

export interface TablePermissionConfig {
  table: string;
  permissions: {
    select?: SelectPermission;
    insert?: InsertPermission;
    update?: UpdatePermission;
    delete?: DeletePermission;
  };
}

// Dynamic Permission Configuration Types
export interface RolePermissions {
  select?: SelectPermission;
  insert?: InsertPermission;
  update?: UpdatePermission;
  delete?: DeletePermission;
}

export interface TablePermissions {
  [roleName: string]: RolePermissions;
}

export interface PermissionConfig {
  tables: {
    [tableName: string]: TablePermissions;
  };
}

// Validation Types
export interface ValidationError {
  path: string;
  message: string;
  code?: string;
}

export interface ValidationResult {
  valid: boolean;
  errors: ValidationError[];
}

// Permission Loader Types
export interface PermissionLoaderOptions {
  resourcesDir?: string;
  fallbackToDefaults?: boolean;
  enableValidation?: boolean;
}

export interface DefaultPermissionProvider {
  getDefaultPermissions(): PermissionConfig;
}

export interface PermissionValidator {
  validateConfig(config: any): ValidationResult;
}

export interface PermissionLoader {
  loadPermissions(): Promise<PermissionConfig>;
}
