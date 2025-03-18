import { CodeAnalysisModel } from '../models/CodeAnalysisModel';

export class CodeAnalysisPresenter {
  constructor() {
    this.model = new CodeAnalysisModel();
    this.state = {
      code: '',
      result: '',
      loading: false,
      error: null
    };
  }

  setCode(code) {
    this.state.code = code;
  }

  async analyzeCode() {
    if (!this.state.code.trim()) {
      this.state.error = 'Please enter some code to analyze';
      return;
    }

    this.state.loading = true;
    this.state.error = null;

    try {
      this.state.result = await this.model.analyzeCode(this.state.code);
    } catch (error) {
      this.state.error = error.message;
    } finally {
      this.state.loading = false;
    }
  }

  getState() {
    return { ...this.state };
  }
} 