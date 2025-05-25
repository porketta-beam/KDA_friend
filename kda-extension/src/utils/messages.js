const vscode = require('vscode');

/**
 * 메시지 배열을 순차적으로 표시하는 유틸리티 함수
 * 메시지는 역순으로 표시되며, 각 메시지 사이에 지연 시간이 있습니다.
 * 
 * @param {string[]} messages - 표시할 메시지 배열
 * @param {string} type - 메시지 타입 ('info' 또는 'warning')
 * @returns {Promise<void>} 모든 메시지가 표시될 때까지 대기
 */
async function showMessages(messages, type = 'info') {
    // 메시지를 역순으로 순회
    for (let i = messages.length - 1; i >= 0; i--) {
        // 메시지 타입에 따라 다른 알림창 표시
        if (type === 'warning') {
            // 경고 메시지로 표시
            await vscode.window.showWarningMessage(messages[i]);
        } else {
            // 정보 메시지로 표시
            await vscode.window.showInformationMessage(messages[i]);
        }
        // 다음 메시지 표시 전 500ms 대기
        await new Promise(resolve => setTimeout(resolve, 500));
    }
}

module.exports = {
    showMessages
}; 