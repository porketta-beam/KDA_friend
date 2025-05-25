const vscode = require('vscode');
const ApiService = require('../services/api');

// 자동 질문 간격 (30초)
const AUTO_QUESTION_INTERVAL = 30000;

class MoluState {
    constructor() {
        this.isAutoQuestionEnabled = false;
        this.autoQuestionTimer = null;
        this.toggleItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 2);
        this.toggleItem.text = '$(terminal-linux)';  // 초기 아이콘: 리눅스 펭귄
        this.toggleItem.color = '#808080';        // 초기 색상: 회색
        this.toggleItem.tooltip = 'molu?';
        this.toggleItem.command = 'kda-extension.molu-toggle';  // 클릭시 실행할 명령어
        this.toggleItem.show();                   // 상태 표시줄에 표시
    }

    async toggleAutoQuestion() {
        this.isAutoQuestionEnabled = !this.isAutoQuestionEnabled;
        
        if (this.isAutoQuestionEnabled) {
            // 토글 ON 상태로 변경
            this.toggleItem.color = '#FFFF00';  // 초록색으로 변경
            this.toggleItem.tooltip = '자동 질문 활성화됨';
            
            // 자동 질문 시작
            await this.startAutoQuestion();
        } else {
            // 토글 OFF 상태로 변경
            this.toggleItem.color = '#808080';  // 회색으로 변경
            this.toggleItem.tooltip = '자동 질문 비활성화됨';
            
            // 자동 질문 중지
            this.stopAutoQuestion();
        }
    }

    async startAutoQuestion() {
        // 즉시 첫 질문 실행
        await ApiService.addQuestion();
        
        // 30초마다 자동 질문 실행
        this.autoQuestionTimer = setInterval(async () => {
            try {
                await ApiService.addQuestion();
            } catch {
                vscode.window.showErrorMessage('자동 질문 전송 실패');
                this.stopAutoQuestion();
            }
        }, AUTO_QUESTION_INTERVAL);
    }

    stopAutoQuestion() {
        if (this.autoQuestionTimer) {
            clearInterval(this.autoQuestionTimer);
            this.autoQuestionTimer = null;
        }
    }

    dispose() {
        if (this.toggleItem) {
            this.toggleItem.dispose();
        }
        this.stopAutoQuestion();
    }
}

module.exports = MoluState; 