// Jupiter交易签名脚本
const { VersionedTransaction } = require('@solana/web3.js');
const bs58 = require('bs58');

// 接收命令行参数
const args = process.argv.slice(2);
const command = args[0]; // 命令: sign, decode, etc.

// 处理错误并以JSON格式输出
function handleError(message) {
  console.log(JSON.stringify({ error: message }));
  process.exit(1);
}

// 签名交易
async function signTransaction(transactionBase64, privateKeyBase58) {
  try {
    if (!transactionBase64) {
      return handleError("Missing transaction data");
    }
    
    if (!privateKeyBase58) {
      return handleError("Missing private key");
    }
    
    // 解码交易
    const transactionBuffer = Buffer.from(transactionBase64, 'base64');
    const privateKeyBytes = bs58.decode(privateKeyBase58);
    
    // 反序列化交易
    const transaction = VersionedTransaction.deserialize(transactionBuffer);
    
    // 创建签名者对象
    const signer = {
      publicKey: transaction.message.staticAccountKeys[0],
      secretKey: privateKeyBytes
    };
    
    // 签名交易
    transaction.sign([signer]);
    
    // 序列化已签名的交易
    const signedTransaction = Buffer.from(transaction.serialize()).toString('base64');
    
    // 输出签名结果
    console.log(JSON.stringify({ 
      signedTransaction,
      success: true
    }));
  } catch (error) {
    handleError(`Transaction signing failed: ${error.message}`);
  }
}

// 主控制流
async function main() {
  try {
    switch (command) {
      case 'sign':
        const transactionBase64 = args[1];
        const privateKeyBase58 = args[2];
        await signTransaction(transactionBase64, privateKeyBase58);
        break;
      default:
        handleError(`Unknown command: ${command}`);
    }
  } catch (error) {
    handleError(`Execution failed: ${error.message}`);
  }
}

main().catch(error => {
  handleError(`Uncaught error: ${error.message}`);
});
