// VSCode API를 가져옵니다
const vscode = require('vscode');

// 각 명령어 클래스를 가져옵니다
const MoluCommand = require('./commands/molu');  // 몰?루 관련 명령어 처리
const DevPostCommand = require('./commands/dev-post');  // 개발 포스트 관련 명령어 처리  
const GaugeCommand = require('./commands/gauge');       // 게이지 관련 명령어 처리
const PadletCommand = require('./commands/padlet');     // Padlet 관련 명령어 처리
const MoluState = require('./commands/molu_state');     // 자동 질문 상태 관리

/**
 * 확장 프로그램이 활성화될 때 호출되는 함수입니다.
 * @param {vscode.ExtensionContext} context - VSCode가 제공하는 확장 프로그램 컨텍스트
 */
function activate(context) {
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 명령어를 만들고 package.json에 반드시 등록해야 함!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    // 각 명령어 클래스의 인스턴스를 생성합니다
    const moluState = new MoluState();
    const moluCommand = new MoluCommand(moluState);
    const devPostCommand = new DevPostCommand(); 
    const gaugeCommand = new GaugeCommand();
    const padletCommand = new PadletCommand();

    // VSCode 명령어 팔레트에 명령어를 등록합니다
    context.subscriptions.push(
        // 몰?루 버튼 명령어 등록
        vscode.commands.registerCommand('kda-extension.molu', () => moluCommand.execute()),
        // 몰?루 자동 질문 토글 명령어 등록
        vscode.commands.registerCommand('kda-extension.molu-toggle', () => moluState.toggleAutoQuestion()),
        // 개발 포스트 명령어 등록
        vscode.commands.registerCommand('kda-extension.dev-post', () => devPostCommand.execute()),
        // 게이지 생성 명령어 등록 
        vscode.commands.registerCommand('kda-extension.create-gauge', () => gaugeCommand.execute()),
        // Padlet 명령어 등록
        vscode.commands.registerCommand('kda-extension.padlet', () => padletCommand.execute()),
        // 각 명령어 인스턴스를 구독 목록에 추가하여 자원 관리
        moluCommand,
        devPostCommand,
        gaugeCommand,
        padletCommand,
        moluState
    );
}

/**
 * 확장 프로그램이 비활성화될 때 호출되는 함수입니다.
 * 필요한 정리 작업을 수행합니다.
 */
function deactivate() {
    // 현재는 특별한 정리 작업이 필요하지 않습니다
}

// 확장 프로그램의 활성화/비활성화 함수를 내보냅니다
module.exports = {
    activate,
    deactivate
}; 