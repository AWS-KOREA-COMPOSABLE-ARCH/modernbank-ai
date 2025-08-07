// const Bedrock = require("@langchain/community/llm/bedrock")
// const StreamingStdOutCallbackHandler = require("langchain/callbacks")
// import { Bedrock } from "l"
const WebSocket = require("ws");
const {
  BedrockRuntimeClient,
  ConverseStreamCommand,
} = require("@aws-sdk/client-bedrock-runtime");
const {
  BedrockAgentRuntimeClient,
  RetrieveCommand,
} = require("@aws-sdk/client-bedrock-agent-runtime");

const AWS_REGION = process.env.AWS_REGION; // AWS 리전
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID; // AWS Access Key
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY; // AWS Secret Key
const BEDROCK_MODEL_ID = "anthropic.claude-3-haiku-20240307-v1:0"; // 사용할 Bedrock Claude 모델 ID
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

// const LangChainbedrockClient = new Bedrock({
//   region: AWS_REGION,
//   accessKeyId: AWS_ACCESS_KEY_ID,
//   secretAccessKey: AWS_SECRET_ACCESS_KEY,
// });

// async function streamConverseClaudeLangchain(ws, message) {
//   if (ws.readyState !== WebSocket.OPEN) {
//     console.error("WebSocket 연결이 끊어졌습니다. 요청을 중단합니다.");
//     return;
//   }

//   // Bedrock 모델 초기화
//   const model = new Bedrock({
//     modelId: BEDROCK_MODEL_ID,
//     client: LangChainbedrockClient,
//     streaming: true,
//     callbacks: [new StreamingStdOutCallbackHandler()],
//     modelKwargs: {
//       maxTokens: MAX_TOKENS,
//       temperature: TEMPERATURE,
//       topP: TOP_P,
//     },
//   });

//   try {
//     console.log("Bedrock 요청 생성 중...");

//     // 프롬프트 설정
//     const prompt = `Human: ${message}\n\nAssistant:`;

//     // 스트리밍 응답 처리
//     const response = await model.call(prompt, {
//       onToken: (token) => {
//         if (ws.readyState !== WebSocket.OPEN) {
//           console.error("WebSocket 연결이 끊어졌습니다. 스트리밍을 중단합니다.");
//           return;
//         }
//         ws.send(token); // WebSocket으로 각 토큰 전송
//       },
//     });

//     if (ws.readyState === WebSocket.OPEN) {
//       console.log("{Stop reason: Done}");
//       ws.send("\\end");
//     }
//   } catch (error) {
//     console.error("Claude 호출 오류:", error);
//     if (ws.readyState === WebSocket.OPEN) {
//       ws.send("AI 응답 생성 중 오류가 발생했습니다.");
//     }
//   }
// }


// WebSocket 서버 생성
const wss = new WebSocket.Server({ port: 5000 }, () => {
  console.log("WebSocket 서버가 포트 5000에서 실행 중입니다.");
});

// Claude Converse API 스트리밍 호출 함수
async function streamConverseClaude(ws, message) {
  if (ws.readyState !== WebSocket.OPEN) {
    console.error("WebSocket 연결이 끊어졌습니다. 요청을 중단합니다.");
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
    console.log("ConverseStreamCommand 요청 생성 중...");

    const commandParams = {
      modelId,
      messages: messages,
      inferenceConfig: {
        maxTokens: 2048,
        temperature: 0.7,
        topP: 0.9,
      },
    };

    // ConverseStreamCommand 생성 및 호출
    const command = new ConverseStreamCommand(commandParams);
    const response = await bedrockClient.send(command);

    console.log("응답 스트림 처리 중...");
    let generatedText = "";

    // 스트리밍 응답 처리
    for await (const item of response.stream) {
      if (ws.readyState !== WebSocket.OPEN) {
        console.error("WebSocket 연결이 끊어졌습니다. 스트리밍을 중단합니다.");
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
    console.error("Claude 호출 오류:", error);
    if (ws.readyState === WebSocket.OPEN) {
      ws.send("AI 응답 생성 중 오류가 발생했습니다.");
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
    // Knowledge Base에서 관련 정보 검색
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
    
    // 검색된 문서들로 컨텍스트 구성
    const context = retrievedPassages
      .map(result => result.content?.text || '')
      .filter(text => text.length > 0)
      .join('\n\n');

    // Claude에 컨텍스트와 함께 메시지 전송
    const contextualMessage = context 
      ? `
      
         금융과 관련된 대화만 검색된 내용으로 답변 합니다. 

         검색된 관련 정보입니다:\n\n${context}\n\n
         질문은 정보 입니다 : ${message}
         
         답변을 하기 전 아래 조건을 참고하여 답변해주세요.
            1. 답변이 검색을 해서 답변을 해야하는지를 판단하세요.
            2. 판단 해야 하는 경우 위 정보를 가지고 답변을 하세요. 
            3. 검색 결과가 필요 없는 것은 직접 답하세요. 
            
        위 조건을 확인해서 답변을 하세요.`
        
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

    // AI 응답 스트리밍
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

    // AI 응답이 완료된 후 참고 문서 전송
    if (retrievedPassages.length > 0) {
      const sources = retrievedPassages.map(result => ({
        text: result.content?.text || '',
        score: result.score,
        location: result.location,
        s3Location: result.location?.s3Location || null,
      })).filter(source => source.text.length > 0);

      // 참고 문서 정보 전송
      ws.send(JSON.stringify({
        type: 'sources',
        content: '참고 문서:',
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
        content: 'AI 응답 생성 중 오류가 발생했습니다.',
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
  console.log("클라이언트가 연결되었습니다.");

  ws.on("message", async (data) => {
    try {
      // 버퍼를 문자열로 변환
      const message = JSON.parse(data.toString());

      console.log("수신된 메시지:", message);
  
      // agent와 text 값 추출
      const { agent, text } = message;
  
      console.log(`Agent: ${agent}, Text: ${text}`);
  
      if (ws.readyState === WebSocket.OPEN) {
        await handleAgentMessage(ws, text, agent);
      } else {
        console.error("WebSocket 연결이 유효하지 않아 메시지를 처리할 수 없습니다.");
      }
    } catch (error) {
      console.error("메시지 처리 중 오류:", error);
      ws.send("유효하지 않은 메시지 형식입니다.");
    }
  });

  ws.on("close", () => {
    console.log("클라이언트 연결이 종료되었습니다.");
  });

  ws.on("error", (error) => {
    console.error("WebSocket 오류:", error);
  });
});
