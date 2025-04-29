const vscode = require('vscode');
const ApiService = require('../services/api');

// 질문 버튼 리셋 시간 (10초)
const RESET_TIME = 10000;

/**
 * 질문 명령어를 처리하는 클래스
 * VSCode 상태 표시줄에 질문 버튼을 생성하고 관리합니다.
 */
class MoluCommand {
    /**
     * MoluCommand 생성자
     * 상태 표시줄에 질문 버튼을 생성하고 초기화합니다.
     */
    constructor() {
        // 상태 표시줄 아이템 생성 (우측 정렬, 우선순위 1)
        this.statusBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 1);
        this.statusBarItem.text = '$(circle-outline)';  // 초기 아이콘: 빈 원
        this.statusBarItem.color = '#00FF00';          // 초기 색상: 초록색
        this.statusBarItem.tooltip = '몰?루';
        this.statusBarItem.command = 'kda-extension.molu';  // 클릭시 실행할 명령어
        this.statusBarItem.show();                     // 상태 표시줄에 표시
        this.moluTimer = null;                     // 리셋 타이머
    }

    /**
     * 질문 버튼 클릭시 실행되는 메서드
     * 질문을 서버에 등록하고 버튼 상태를 변경합니다.
     */
    async execute() {
        try {
            // 이미 질문 상태인 경우 메시지만 표시
            if (this.statusBarItem.text === '$(question)') {
                vscode.window.showInformationMessage('?');
                return;
            }

            // 서버에 질문 추가
            await ApiService.addQuestion();
            
            // 버튼 상태 변경 (물음표 아이콘, 노란색)
            this.statusBarItem.text = '$(question)';
            this.statusBarItem.color = '#FFFF00';
            
            // 기존 타이머가 있다면 제거
            if (this.moluTimer) {
                clearTimeout(this.moluTimer);
            }
            // 일정 시간 후 버튼 상태 초기화
            this.moluTimer = setTimeout(() => {
                this.statusBarItem.text = '$(circle-outline)';
                this.statusBarItem.color = '#00FF00';
            }, RESET_TIME);
        } catch {
            // 오류 발생시 에러 메시지 표시
            vscode.window.showErrorMessage('오류발생.. slack으로 문의 바람');
        }
    }

    /**
     * 리소스 정리 메서드
     * 상태 표시줄 아이템과 타이머를 정리합니다.
     */
    dispose() {
        this.statusBarItem.dispose();  // 상태 표시줄 아이템 제거
        if (this.moluTimer) {
            clearTimeout(this.moluTimer);  // 타이머 정리
        }
    }
}

module.exports = MoluCommand;