const vscode = require('vscode');
const ApiService = require('./api');
const { showMessages } = require('../utils/messages');

// 게이지 업데이트 간격 (밀리초 단위)
const GAUGE_UPDATE_INTERVAL = 1000; // 1초

/**
 * 게이지 서비스 클래스
 * VSCode 상태 표시줄에 학생들의 이해도를 시각적으로 표시하는 게이지를 관리합니다.
 */
class GaugeService {
    /**
     * GaugeService 생성자
     * 게이지 관련 상태 변수들을 초기화합니다.
     */
    constructor() {
        this.gaugeBarItem = null;          // 상태 표시줄 아이템
        this.gaugeUpdateInterval = null;    // 업데이트 인터벌 ID
        this.currentQuestionCount = 0;      // 현재 질문 수
        this.previousValue = 0;             // 이전 질문 수 (메시지 중복 방지용)
        this.message50Shown = false;        // 50% 도달 메시지 표시 여부
        this.message75Shown = false;        // 75% 도달 메시지 표시 여부
        this.message100Shown = false;       // 100% 도달 메시지 표시 여부
        this.isActive = false;              // 게이지 활성화 상태
    }

    /**
     * 게이지 바를 생성하고 초기화합니다.
     * 이미 존재하는 게이지는 제거하고 새로 생성합니다.
     * @returns {vscode.StatusBarItem} 생성된 상태 표시줄 아이템
     */
    createGaugeBar() {
        if (this.gaugeBarItem) {
            this.gaugeBarItem.dispose();
            this.gaugeBarItem = null;
        }

        this.gaugeBarItem = vscode.window.createStatusBarItem(vscode.StatusBarAlignment.Right, 10000);
        this.gaugeBarItem.text = '$(loading~spin)';
        this.gaugeBarItem.show();

        if (this.gaugeUpdateInterval) {
            clearInterval(this.gaugeUpdateInterval);
        }
        this.gaugeUpdateInterval = setInterval(() => this.updateGauge(), GAUGE_UPDATE_INTERVAL);
        this.updateGauge();
        this.isActive = true;

        return this.gaugeBarItem;
    }

    /**
     * 게이지 상태를 주기적으로 업데이트합니다.
     * API를 통해 현재 질문 수를 가져와서 게이지를 갱신하고,
     * 특정 임계값에 도달하면 알림 메시지를 표시합니다.
     */
    async updateGauge() {
        try {
            const value = await ApiService.getQuestionCount();
            
            if (!this.gaugeBarItem) {
                return;
            }

            this.currentQuestionCount = value;
            // 질문 수를 0~1 사이의 값으로 정규화 (최대 37명 기준)
            const normalizedValue = Math.min(Math.max(value / 37, 0), 1);
            // 20칸 기준으로 채워질 블록과 빈 블록 계산
            const filledBlocks = Math.floor(normalizedValue * 20);
            const emptyBlocks = 20 - filledBlocks;
            // 게이지 바 텍스트 업데이트 (█: 채워진 블록, ░: 빈 블록)
            this.gaugeBarItem.text = `${filledBlocks > 0 ? '█'.repeat(filledBlocks) : ''}${emptyBlocks > 0 ? '░'.repeat(emptyBlocks) : ''}`;
            this.gaugeBarItem.color = '#E74C3C';

            // 100% 도달 시 메시지
            if (normalizedValue >= 1 && value > this.previousValue && !this.message100Shown) {
                await showMessages([
                    '🚨 강사님! 🚨',
                    '머리가 터질 것 같아요...',
                    '잠깐 쉬었다 할까요...? ㅎㅎ'
                ], 'warning');
                this.message100Shown = true;
            }
            // 75% 도달 시 메시지
            else if (normalizedValue > 0.75 && value > this.previousValue && !this.message75Shown) {
                await showMessages([
                    '😭 강사님 살려주세요... 😭',
                    '여기만 넘어가면 잘 할 수 있을 것 같아요 ㅠㅠ',
                    '현재 잘 모르겠는 학생 : ' + value + '명'
                ], 'warning');
                this.message75Shown = true;
            }
            // 50% 도달 시 메시지
            else if (normalizedValue > 0.5 && value > this.previousValue && !this.message50Shown) {
                await vscode.window.showInformationMessage('💡 강사님 조금 버거워요... 💡 \n'
                    +'한 번만 다시 설명해 주시면 이해될 것 같아요!'
                    +' \n 현재 잘 모르는 학생 : ' + value + '명');
                this.message50Shown = true;
            }

            this.previousValue = value;
        } catch (error) {
            console.error('게이지 업데이트 실패:', error);
            if (this.gaugeBarItem) {
                this.gaugeBarItem.text = '게이지 업데이트 실패';
            }
        }
    }

    /**
     * 게이지 서비스의 자원을 정리합니다.
     * 상태 표시줄 아이템과 업데이트 인터벌을 제거합니다.
     */
    dispose() {
        if (this.gaugeBarItem) {
            this.gaugeBarItem.dispose();
            this.gaugeBarItem = null;
        }
        if (this.gaugeUpdateInterval) {
            clearInterval(this.gaugeUpdateInterval);
            this.gaugeUpdateInterval = null;
        }
        this.isActive = false;
    }
}

module.exports = GaugeService; 