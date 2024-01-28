// Follow this setup guide to integrate the Deno language server with your editor:
// https://deno.land/manual/getting_started/setup_your_environment
// This enables autocomplete, go to definition, etc.
import { createClient } from "https://esm.sh/@supabase/supabase-js@2";
import { GlacierClient } from "https://esm.sh/@glacier-network/client";
import { Application, Router, send } from "https://deno.land/x/oak/mod.ts";
import { oakCors } from "https://deno.land/x/cors/mod.ts";

console.log("Hello from glacier syncer!");

/* public functions */

//  const arweaveEndpoint = 'https://p0.onebitdev.com/glacier-gateway';
//  const filEndpoint = 'https://web3storage.onebitdev.com/glacier-gateway';
const greenfieldEndpoint =
  "https://greenfield.onebitdev.com/glacier-gateway-vector";

const endpoint = greenfieldEndpoint;

const MoveSpaceAPIKey = Deno.env.get("MOVESPACE_API_KEY");
const myNamespaceStr = Deno.env.get("NAMESPACE");
const privateKey = Deno.env.get("PRIVATE_KEY");

function toCamelCase(datasetName) {
  return datasetName
    .split("_")
    .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
    .join("");
}

// function toKebabCase(datasetName) {
//   return datasetName.replace(/([a-z])([A-Z])/g, "$1-$2").toLowerCase();
// }

interface RecordItem {
  title: string;
  content: string;
  createdAt?: number;
  updatedAt?: number;
}

function transformArrayToJSON(array: any[]) {
  const json = {};

  array.forEach((item) => {
    let key = item.target_column_name;
    let type = "string"; // default type for most fields
    if (key === "data") {
      json[key] = {
        type: type,
        maxLength: 5000,
      };
      // Special handling for 'data' field
      json["Embedding"] = {
        type: "array",
        vectorIndexOption: {
          type: "knnVector",
          dimensions: 1536,
          // 1536 for OpenAI text-embedding-ada-002 & text-embedding-3-small vector
          // dimensions: 384,
          // 384 for all-MiniLM-L6-v2 Embedding.
          similarity: "euclidean",
        },
      };
    } else {
      // Convert 'created_at' and 'updated_at' to 'number' type
      if (key === "created_at" || key === "updated_at") {
        type = "number";
      }
      if (key != "embedding") {
        // Convert column name to camelCase (if needed)
        key = toCamelCase(key);
        console.log("key:", key);
        // Adding the transformed key and type to the JSON object
        json[key] = {
          type: type,
          maxLength: 5000,
        };
      }
    }
  });

  return json;
}

/* router */

const router = new Router();
router
  .post("/create", async (context) => {
    let content = await context.request.body.text();
    content = JSON.parse(content);
    console.log("content", content);
    // console.log('body', JSON.parse(body).tx_id);
    // const content = await context.request.body().value;
    const datasetName = content.dataset;
    const datasetNameCamel = toCamelCase(datasetName);
    console.log("dataset_name", datasetName);
    const supabase = createClient(
      // Supabase API URL - env var exported by default.
      Deno.env.get("SUPABASE_URL") ?? "",
      // Supabase API ANON KEY - env var exported by default.
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
      // Create client with Auth context of the user that called the function.
      // This way your row-level-security (RLS) policies are applied.
      // { global: { headers: { Authorization: req.headers.get('Authorization')! } } }
    );

    const client = new GlacierClient(endpoint, {
      privateKey,
    });

    // if table not exist, create table in glacier.
    const myNamespace = client.namespace(myNamespaceStr);

    /* Functions */
    async function createNamespace() {
      const result = await client.createNamespace(myNamespaceStr);
      console.log("Namespace created:", result);
    }

    async function createDataset(datasetName) {
      const result = await myNamespace.createDataset(datasetName);
      console.log("dataset created:", result);
    }

    async function datasetExists(datasetName: string): Promise<boolean> {
      try {
        const result = await myNamespace.queryDataset(datasetName);
        console.log(result);
        return result != null; // assuming queryDataset returns null if dataset doesn't exist
      } catch (error) {
        console.error("Error checking dataset existence:", error);
        return false;
      }
    }

    async function createDatasetIfNotExists(datasetName: string) {
      const exists = await datasetExists(datasetName);
      if (!exists) {
        console.log(`Dataset ${datasetName} does not exist, creating...`);
        await createDataset(datasetName);
      } else {
        console.log(`Dataset ${datasetName} already exists.`);
      }
    }

    async function createCollection() {
      const result = await client
        .namespace(myNamespaceStr)
        .dataset(toCamelCase(datasetName))
        .createCollection(toCamelCase(datasetName), {
          title: toCamelCase(datasetName),
          type: "object",
          properties: transformArrayToJSON(fieldDetails),
          required: ["data", "metadata"],
        });
      console.log("createCollection result:", result);
    }

    /* Commands */
    await createNamespace();
    await createDatasetIfNotExists(datasetNameCamel);
    const { data: fieldDetails, error_2 } = await supabase.rpc(
      "get_field_details",
      { target_table_name: datasetName }
    );
    console.log("get field details", fieldDetails);

    console.log("properties", transformArrayToJSON(fieldDetails));

    createCollection();

    return new Response(JSON.stringify({ finished: true }), {
      headers: { "Content-Type": "application/json" },
    });
  })
  .post("/delete", async (context) => {
    let content = await context.request.body.text();
    content = JSON.parse(content);
    const datasetName = content.dataset;
    console.log("dataset_name", datasetName);
    const uuid = content.uuid;
    console.log("uuid", uuid);

    const client = new GlacierClient(endpoint, {
      privateKey,
    });
    const myNamespace = client.namespace(myNamespaceStr);
    const myDataset = myNamespace.dataset(toCamelCase(datasetName));
    const myCollection = myDataset.collection<RecordItem>(
      toCamelCase(datasetName)
    );
    async function deleteOne() {
      const result = await myCollection.deleteOne({
        id: uuid,
      });
      console.log("delete result:", result);
    }
    await deleteOne();
    context.response.body = "delete success!";
  })
  .post("/insert", async (context) => {
    let content = await context.request.body.text();
    content = JSON.parse(content);
    const datasetName = content.dataset;
    console.log("dataset_name", datasetName);
    const uuid = content.uuid;
    console.log("uuid", uuid);
    const supabase = createClient(
      // Supabase API URL - env var exported by default.
      Deno.env.get("SUPABASE_URL") ?? "",
      // Supabase API ANON KEY - env var exported by default.
      Deno.env.get("SUPABASE_SERVICE_ROLE_KEY") ?? ""
      // Create client with Auth context of the user that called the function.
      // This way your row-level-security (RLS) policies are applied.
      // { global: { headers: { Authorization: req.headers.get('Authorization')! } } }
    );

    const { data, error } = await supabase
      .from(datasetName)
      .select()
      .eq("uuid", uuid);
    console.log(data);

    const client = new GlacierClient(endpoint, {
      privateKey,
    });
    const myNamespace = client.namespace(myNamespaceStr);
    const myDataset = myNamespace.dataset(toCamelCase(datasetName));
    const myCollection = myDataset.collection<RecordItem>(
      toCamelCase(datasetName)
    );

    function transformKeysToCamelCase(obj) {
      let newObject = {};

      for (let key in obj) {
        let newKey = toCamelCase(key);

        // Special handling for nested objects (like 'metadata')
        if (
          typeof obj[key] === "object" &&
          obj[key] !== null &&
          !Array.isArray(obj[key])
        ) {
          newObject[newKey] = transformKeysToCamelCase(obj[key]);
        } else {
          console.log("obj keys", obj[key]);
          newObject[newKey] = obj[key];
        }
      }

      return newObject;
    }

    // insert data[0] into dataset in glacier.
    // tricky handle.
    function transformData(record) {
      // Function to convert ISO 8601 date format to Unix timestamp
      function toUnixTimestamp(isoDate) {
        return new Date(isoDate).getTime();
      }

      // Converting 'created_at' and 'updated_at' to Unix timestamps
      if ("created_at" in record) {
        record.created_at = toUnixTimestamp(record.created_at);
      }
      if ("updated_at" in record) {
        record.updated_at = toUnixTimestamp(record.updated_at);
      } else {
        record.updated_at = Date.now();
      }

      // Converting 'metadata' to a JSON string
      record.metadata = JSON.stringify(record.metadata);

      return record;
    }
    const finalData = transformKeysToCamelCase(transformData(data[0]));
    finalData["Embedding"] = JSON.parse(data[0]["embedding"]);
    finalData["IfGlacier"] = true;
    console.log("finalData", finalData);
    const result = await myCollection.insertOne(finalData);
    console.log("insert result:", result);
    context.response.body = result;
  })
  .post("/search", async (context) => {
    let content = await context.request.body.text();
    content = JSON.parse(content);
    const datasetName = content.dataset;
    const question = content.question;
    // Step1. Generate Embedding for the search data by MoveSpace.
    let response = await fetch(
      "https://faas.movespace.xyz/api/v1/run?name=VectorAPI&func_name=get_embedding",
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ params: [MoveSpaceAPIKey, question] }),
      }
    );

    response = await response.json();
    console.log("embedding:", response.result.data.vector);
    const client = new GlacierClient(endpoint, {
      privateKey,
    });

    function toCamelCase(datasetName) {
      return datasetName
        .split("_")
        .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
        .join("");
    }

    const datasetNameActually = toCamelCase(datasetName);
    // get the collection
    let coll = client
      .namespace(myNamespaceStr)
      .dataset(datasetNameActually)
      .collection(datasetNameActually);
    console.log(coll);
    // Step2. Saving search in deno KV.
    // Step3. Search the embedding in glacier.

    console.log(datasetName);
    // find the result
    let result = await coll
      .find({
        numCandidates: 10,
        vectorPath: "Embedding",
        queryVector: JSON.parse(response.result.data.vector),
      })
      .toArray();
    console.log("result", result);
  });

const app = new Application();
app.use(oakCors()); // Enable CORS for All Routes
app.use(router.routes());

console.info("CORS-enabled web server listening on port 8000");

await app.listen({ port: 8000 });
