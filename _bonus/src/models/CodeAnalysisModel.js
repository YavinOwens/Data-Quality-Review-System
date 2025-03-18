import axios from 'axios';

export class CodeAnalysisModel {
  constructor() {
    this.apiUrl = 'http://localhost:11434/api/generate';
  }

  async analyzeCode(code) {
    try {
      const response = await axios.post(this.apiUrl, {
        model: 'phi3.5:latest',
        prompt: `Analyze this code and provide detailed feedback:
\`\`\`
${code}
\`\`\``,
        stream: false
      });
      return response.data.response;
    } catch (error) {
      throw new Error(error.message);
    }
  }
} 