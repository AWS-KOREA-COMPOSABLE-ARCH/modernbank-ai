const WebSocket = require("ws");
const {
  BedrockRuntimeClient,
  ConverseStreamCommand,
} = require("@aws-sdk/client-bedrock-runtime");
const {
  BedrockAgentRuntimeClient,
  RetrieveCommand,
} = require("@aws-sdk/client-bedrock-agent-runtime");

const AWS_REGION = process.env.AWS_REGION; // AWS Region
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID; // AWS Access Key
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY; // AWS Secret Key
const BEDROCK_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"; // Bedrock Claude Model ID to use
const MAX_TOKENS = 2048;
const TEMPERATURE = 0.7;
const TOP_P = 0.9;


const KB_CONFIG = {
  knowledgeBaseId: "O0A05WU00W",
  s3Bucket: "composable-knowledge-base-document-216989108269",
  region: "ap-northeast-2"
};

// Initialize Bedrock clients
const bedrockClient = new BedrockRuntimeClient({
  region: AWS_REGION,
  accessKeyId: AWS_ACCESS_KEY_ID,
  secretAccessKey: AWS_SECRET_ACCESS_KEY,
});

const agentClient = new BedrockAgentRuntimeClient({
  region: AWS_REGION,
  accessKeyId: AWS_ACCESS_KEY_ID,
  secretAccessKey: AWS_SECRET_ACCESS_KEY,
});



// Create WebSocket server
const wss = new WebSocket.Server({ port: 5000 }, () => {
  console.log("WebSocket server is running on port 5000.");
});

// Claude Converse API streaming call function
async function streamConverseClaude(ws, message) {
  if (ws.readyState !== WebSocket.OPEN) {
    console.error("WebSocket connection is closed. Aborting request.");
    return;
  }

  const modelId = "anthropic.claude-3-haiku-20240307-v1:0";
  const messages = [
    {
      role: "user",
      content: [
        {
          text: `Human: ${message}\n\nAssistant:`,
        },
      ],
    },
  ];

  try {
    console.log("Creating ConverseStreamCommand request...");

    const commandParams = {
      modelId,
      messages: messages,
      inferenceConfig: {
        maxTokens: 2048,
        temperature: 0.7,
        topP: 0.9,
      },
    };

    // Create and call ConverseStreamCommand
    const command = new ConverseStreamCommand(commandParams);
    const response = await bedrockClient.send(command);

    console.log("Processing response stream...");
    let generatedText = "";

    // Handle streaming response
    for await (const item of response.stream) {
      if (ws.readyState !== WebSocket.OPEN) {
        console.error("WebSocket connection is closed. Stopping streaming.");
        break;
      }

      if (item.contentBlockDelta && item.contentBlockDelta.delta) {
        const text = item.contentBlockDelta.delta.text;
        if (text) {
          generatedText += text;
          ws.send(text);
        }
      }
    }

    if (ws.readyState === WebSocket.OPEN) {
      console.log("{Stop reason: Done}");
      ws.send("\\end");
    }
  } catch (error) {
    console.error("Claude call error:", error);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send("An error occurred while generating AI response.");
    }
  }
}

// Add new RAG Chat function
async function streamRAGChat(ws, message) {
  if (ws.readyState !== WebSocket.OPEN) {
    console.error("WebSocket connection is closed");
    return;
  }

  try {
    // Search for relevant information from Knowledge Base
    const retrieveCommand = new RetrieveCommand({
      knowledgeBaseId: KB_CONFIG.knowledgeBaseId,
      retrievalQuery: {
        text: message
      },
      retrievalConfiguration: {
        vectorSearchConfiguration: {
          numberOfResults: 3
        }
      }
    });

    const retrievalResponse = await agentClient.send(retrieveCommand);
    const retrievedPassages = retrievalResponse.retrievalResults || [];
    
    // Build context from retrieved documents
    const context = retrievedPassages
      .map(result => result.content?.text || '')
      .filter(text => text.length > 0)
      .join('\n\n');

    // Send message to Claude with context
    const contextualMessage = context 
      ? `
      
         Answer only Financial Service Industry-related conversations using the retrieved content. 

         Here is the retrieved relevant information:\n\n${context}\n\n
         The question is: ${message}
         
         Please refer to the following conditions before answering:
            1. Determine if the answer requires search results.
            2. If it requires search, answer using the above information. 
            3. If search results are not needed, answer directly. 
            4. Detect the language of the incoming question and respond in the same language. 
               (For example, if the question is in English, answer in English; if it is in Korean, answer in Korean.)
         Please check the above conditions and provide your answer.`
        
      : message;
    
    const messages = [{
      role: "user",
      content: [{ text: `Human: ${contextualMessage}\n\nAssistant:` }],
    }];

    const commandParams = {
      modelId: BEDROCK_MODEL_ID,
      messages: messages,
      inferenceConfig: {
        maxTokens: MAX_TOKENS,
        temperature: TEMPERATURE,
        topP: TOP_P,
      },
    };

    const command = new ConverseStreamCommand(commandParams);
    const response = await bedrockClient.send(command);
    let fullResponse = '';

    // Stream AI response
    for await (const item of response.stream) {
      if (ws.readyState !== WebSocket.OPEN) break;
      
      if (item.contentBlockDelta?.delta?.text) {
        const text = item.contentBlockDelta.delta.text;
        fullResponse += text;
        ws.send(JSON.stringify({
          type: 'text',
          content: text
        }));
      }
    }

    // Send reference documents after AI response is complete
    if (retrievedPassages.length > 0) {
      const sources = retrievedPassages.map(result => ({
        text: result.content?.text || '',
        score: result.score,
        location: result.location,
        s3Location: result.location?.s3Location || null,
      })).filter(source => source.text.length > 0);

      // Send reference document information
      ws.send(JSON.stringify({
        type: 'sources',
        content: 'Reference Documents:',
        metadata: {
          sources,
          aiResponse: fullResponse
        }
      }));
    }

    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'end',
        content: '\\end',
        metadata: {
          hasReferences: retrievedPassages.length > 0
        }
      }));
    }
  } catch (error) {
    console.error("RAG Chat error:", error);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'error',
        content: 'An error occurred while generating AI response.',
        metadata: {
          error: error.message
        }
      }));
    }
  }
}

// Update handleAgentMessage function
async function handleAgentMessage(ws, message, agent) {
  switch (agent) {
    case "rag":
      console.log("Using RAG Chat");
      await streamRAGChat(ws, message);
      break;
    default:
      console.log("Using default agent");
      await streamRAGChat(ws, message);
  }
}

wss.on("connection", (ws) => {
  console.log("Client connected.");

  ws.on("message", async (data) => {
    try {
      // Convert buffer to string
      const message = JSON.parse(data.toString());

      console.log("Received message:", message);
  
      // Extract agent and text values
      const { agent, text } = message;
  
      console.log(`Agent: ${agent}, Text: ${text}`);
  
      if (ws.readyState === WebSocket.OPEN) {
        await handleAgentMessage(ws, text, agent);
      } else {
        console.error("WebSocket connection is not valid, cannot process message.");
      }
    } catch (error) {
      console.error("Error processing message:", error);
      ws.send("Invalid message format.");
    }
  });

  ws.on("close", () => {
    console.log("Client connection closed.");
  });

  ws.on("error", (error) => {
    console.error("WebSocket error:", error);
  });
});
