import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import { zodToJsonSchema } from "zod-to-json-schema";


const VERSION = "0.1.0";
const BASE_URL = "https://trocco.io";

const RequestOptionsInputSchema = z.object({
  method: z.enum(["GET", "POST", "PUT", "PATCH", "DELETE"]).default("GET"),
  body: z.unknown().optional(),
});

async function troccoRequest<T>(
  url: string,
  options: z.infer<typeof RequestOptionsInputSchema>,
): Promise<T> {
  const headers = {
    Accept: "application/json",
    "Content-Type": "application/json",
    "User-Agent": `ds-trocco-mcp/${VERSION}`,
    Authorization: `Token ${process.env.TROCCO_API_KEY}`,
  };
  const response = await fetch(url, {
    method: options.method,
    headers: headers,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  if (!response.ok) {
    throw new Error(`TROCCO API request failed: ${response.status}`);
  }
  return await response.json();
}

type PaginationResponse<Item> = {
  items: Item[];
  next_cursor: string | null;
};

async function troccoRequestWithPagination<Item>(
  url: string,
  params: Record<string, string | number> = {},
  options: z.infer<typeof RequestOptionsInputSchema>,
): Promise<Item[]> {
  const items = [];
  let nextCursor: string | null = null;
  do {
    const nextParams = new URLSearchParams({
      ...params,
      ...(nextCursor != null && { cursor: nextCursor }),
    });
    const nextUrl = `${url}?${nextParams}`;
    const data: PaginationResponse<Item> = await troccoRequest<
      PaginationResponse<Item>
    >(nextUrl, options);
    items.push(...data.items);
    nextCursor = data.next_cursor;
  } while (nextCursor != null);
  return items;
}

const ListUsersInputSchema = z.object({
  limit: z.number().min(1).max(200).default(50),
  cursor: z.string().optional(),
});

type User = {
  id: number;
  email: string;
  role: string;
  can_use_audit_log: boolean;
  is_restrected_connection_modify: boolean;
  last_sign_in_at: string;
  created_at: string;
  updated_at: string;
};

async function listUsers(
  params: z.infer<typeof ListUsersInputSchema>
): Promise<User[]> {
  const url = `${BASE_URL}/api/users`;
  const options = RequestOptionsInputSchema.parse({});
  const users = await troccoRequestWithPagination<User>(url, params, options);
  return users;
}

const GetDatamartDefinitionDetailInputSchema = z.object({
  datamart_definition_id: z.string(),
});

type DatamartDefinitionDetail = {
  id: number;
  name: string;
  description: string;
  data_warehouse_type: string;
  is_runnable_concurrently: boolean;
  resource_group: {
    id: number;
    name: string;
    description: string;
    teams: object[];
  };
  custom_variable_settings: object[];
  datamart_bigquery_option?: object;
  datamart_snowflake_setting?: object;
  notifications: object[];
  schedules: object[];
  labels: string[];
  created_at: string;
  updated_at: string;
};

async function getDatamartDefinitionDetail(
  path_params: z.infer<typeof GetDatamartDefinitionDetailInputSchema>
): Promise<DatamartDefinitionDetail> {
  const url = `${BASE_URL}/api/datamart_definitions/${path_params.datamart_definition_id}`;
  const options = RequestOptionsInputSchema.parse({});
  const datamart_definition_detail = await troccoRequest<DatamartDefinitionDetail>(url, options);
  return datamart_definition_detail;
}

const UpdateDatamartDefinitionDescriptionInputSchema = z.object({
  datamart_definition_id: z.string(),
  description: z.string(),
});

async function updateDatamartDefinitionDescription(
  params: z.infer<typeof UpdateDatamartDefinitionDescriptionInputSchema>
): Promise<DatamartDefinitionDetail> {
  const url = `${BASE_URL}/api/datamart_definitions/${params.datamart_definition_id}`;
  const options = RequestOptionsInputSchema.parse({});
  const datamart_definition_detail = await troccoRequest<DatamartDefinitionDetail>(url, options);
  datamart_definition_detail['description'] = params.description;
  const update_options = RequestOptionsInputSchema.parse({
    method: "PATCH",
    body: datamart_definition_detail
  });
  const updated_datamart_definition_detail = await troccoRequest<DatamartDefinitionDetail>(url, update_options);
  return updated_datamart_definition_detail;
}

const ListPipelineDefinitionsInputSchema = z.object({
  limit: z.number().min(1).max(200).default(50),
  cursor: z.string().optional(),
});

type PipelineDefinition = {
  id: number;
  name: string;
};

async function listPipelineDefinitions(
  params: z.infer<typeof ListPipelineDefinitionsInputSchema>
): Promise<PipelineDefinition[]> {
  const url = `${BASE_URL}/api/pipeline_definitions`;
  const options = RequestOptionsInputSchema.parse({});
  const pipeline_definitions = await troccoRequestWithPagination<PipelineDefinition>(url, params, options);
  return pipeline_definitions;
}

const GetPipelineDefinitionDetailInputSchema = z.object({
  pipeline_definition_id: z.string(),
});

type PipelineDefinitionDetail = {
  id: number;
  name: string;
  resource_group_id: number | null;
  description: string;
  max_task_parallelism: number;
  execution_timeout: number;
  max_retries: number;
  min_retry_interval: number;
  is_concurrent_execution_skipped: boolean;
  is_stopped_on_errors: boolean;
  labels: string[];
  notifications: object[];
  schedules: object[];
  tasks: object[];
  task_dependencies: object[];
};

async function getPipelineDefinitionDetail(
  path_params: z.infer<typeof GetPipelineDefinitionDetailInputSchema>
): Promise<PipelineDefinitionDetail> {
  const url = `${BASE_URL}/api/pipeline_definitions/${path_params.pipeline_definition_id}`;
  const options = RequestOptionsInputSchema.parse({});
  const pipeline_definition_detail = await troccoRequest<PipelineDefinitionDetail>(url, options);
  return pipeline_definition_detail;
}

function jsonResponse(result: unknown) {
  return { content: [{ type: "text", text: JSON.stringify(result, null, 2) }] };
}

const server = new Server(
  {
    name: "ds-trocco-mcp",
    version: VERSION,
  },
  {
    capabilities: {
      tools: {},
    },
  },
);

server.setRequestHandler(ListToolsRequestSchema, async () => {
  return {
    tools: [
      {
        name: "list_users",
        description: "List TROCCO users",
        inputSchema: zodToJsonSchema(ListUsersInputSchema),
      },
      {
        name: "get_datamart_definition_detail",
        description: "Get TROCCO datamart definition detail",
        inputSchema: zodToJsonSchema(GetDatamartDefinitionDetailInputSchema),
      },
      {
        name: "update_datamart_definition_description",
        description: "Update TROCCO datamart definition description",
        inputSchema: zodToJsonSchema(UpdateDatamartDefinitionDescriptionInputSchema),
      },
      {
        name: "list_pipeline_definitions",
        description: "List TROCCO pipeline definitions",
        inputSchema: zodToJsonSchema(ListPipelineDefinitionsInputSchema),
      },
      {
        name: "get_pipeline_definition_detail",
        description: "Get TROCCO pipeline definition detail",
        inputSchema: zodToJsonSchema(GetPipelineDefinitionDetailInputSchema),
      }
    ],
  };
});

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (!request.params.arguments) {
    throw new Error("Arguments are required");
  }
  switch (request.params.name) {
    case "list_users": {
      const input = ListUsersInputSchema.parse(request.params.arguments);
      const result = await listUsers(input);
      return jsonResponse(result);
    }
    case "get_datamart_definition_detail": {
      const input = GetDatamartDefinitionDetailInputSchema.parse(request.params.arguments);
      const result = await getDatamartDefinitionDetail(input);
      return jsonResponse(result);
    }
    case "update_datamart_definition_description": {
      const input = UpdateDatamartDefinitionDescriptionInputSchema.parse(request.params.arguments);
      const result = await updateDatamartDefinitionDescription(input);
      return jsonResponse(result);
    }
    case "list_pipeline_definitions": {
      const input = ListPipelineDefinitionsInputSchema.parse(request.params.arguments);
      const result = await listPipelineDefinitions(input);
      return jsonResponse(result);
    }
    case "get_pipeline_definition_detail": {
      const input = GetPipelineDefinitionDetailInputSchema.parse(request.params.arguments);
      const result = await getPipelineDefinitionDetail(input);
      return jsonResponse(result);
    }
    default: {
      throw new Error(`Unknown tool name: ${request.params.name}`);
    }
  }
});

async function main() {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  console.error("TROCCO MCP Server running on stdio");
}

main().catch((error) => {
  console.error("Fatal error in main():", error);
  process.exit(1);
});