const axios = require('axios');

/**
 * API 서버의 기본 URL
 * KDA 서버의 엔드포인트를 정의합니다.
 */
const BASE_URL = 'http://54.180.117.184:8000';

/**
 * API 요청을 처리하는 서비스 클래스
 * 서버와의 통신을 담당하며 질문 관련 기능을 제공합니다.
 */
class ApiService {
    /**
     * 새로운 질문을 서버에 추가하는 메서드
     * POST 요청을 통해 molu 상태임을 전송합니다.
     * @returns {Promise} 서버 응답을 반환합니다.
     * @throws {Error} API 요청 실패시 에러를 발생시킵니다.
     */
    static async addQuestion() {
        try {
            return await axios.post(`${BASE_URL}/add_question`);
        } catch (error) {
            console.error('질문 추가 실패:', error);
            throw new Error('질문 추가에 실패했습니다.');
        }
    }

    /**
     * 현재 등록된 질문의 수를 조회하는 메서드
     * GET 요청을 통해 질문 수를 가져옵니다.
     * @returns {Promise<number>} 현재 질문 수를 반환합니다.
     * @throws {Error} API 요청 실패 또는 잘못된 응답 형식일 경우 에러를 발생시킵니다.
     */
    static async getQuestionCount() {
        try {
            const response = await axios.get(`${BASE_URL}/get_question`);
            const count = Number(response.data.current_count);
            if (isNaN(count)) {
                throw new Error('잘못된 응답 형식입니다.');
            }
            return count;
        } catch (error) {
            console.error('질문 수 조회 실패:', error);
            throw new Error('질문 수를 가져오는데 실패했습니다.');
        }
    }
}

module.exports = ApiService;