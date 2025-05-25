const vscode = require('vscode');
const GaugeService = require('../services/gauge');

/**
 * 게이지 명령어를 처리하는 클래스
 * VSCode 상태 표시줄에 이해도 게이지를 생성하고 관리합니다.
 */
class GaugeCommand {
    /**
     * GaugeCommand 생성자
     * GaugeService 인스턴스를 생성하여 초기화합니다.
     */
    constructor() {
        this.gaugeService = new GaugeService();
    }

    /**
     * 게이지 표시/숨김을 토글하는 메서드
     * 게이지가 없으면 생성하고, 있으면 제거합니다.
     */
    async execute() {
        if (!this.gaugeService.gaugeBarItem) {
            // 게이지바가 없는 경우 새로 생성
            this.gaugeService.createGaugeBar();
            await vscode.window.showInformationMessage('게이지바가 표시되었습니다.');
        } else {
            // 게이지바가 있는 경우 제거
            this.gaugeService.dispose();
            await vscode.window.showInformationMessage('게이지바가 숨겨졌습니다.');
        }
    }

    /**
     * 리소스 정리 메서드
     * GaugeService의 리소스를 정리합니다.
     */
    dispose() {
        this.gaugeService.dispose();
    }
}

module.exports = GaugeCommand;