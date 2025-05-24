const vscode = require('vscode');
const ApiService = require('../services/api');

/**
 * 개발용 POST 요청을 처리하는 클래스
 * 테스트 목적으로 서버에 질문을 추가합니다.
 */
class DevPostCommand {
    /**
     * POST 요청을 실행하는 메서드
     * 서버에 질문을 추가하고 결과를 알림으로 표시합니다.
     */
    async execute() {
        try {
            // 서버에 질문 추가 요청
            await ApiService.addQuestion();
            // 성공 메시지 표시
            vscode.window.showInformationMessage('개발용 post 요청 성공');
        } catch {
            // 오류 발생시 에러 메시지 표시
            vscode.window.showErrorMessage('오류발생.. slack으로 문의 바람');
        }
    }
}

module.exports = DevPostCommand;