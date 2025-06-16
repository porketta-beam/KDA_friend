import { createClient } from '@supabase/supabase-js'

// Create a single supabase client for interacting with your database
const supabase = createClient(import.meta.env.VITE_SUPABASE_URL, import.meta.env.VITE_SUPABASE_ANON_KEY)

/**
 * valu 버킷에서 파일을 다운로드하는 함수
 * @param {string} dir - 다운로드할 파일 경로
 * @returns {Promise<{data: Blob, error: Error}>} 다운로드된 파일 데이터와 에러 객체
 * @example
 * const { data, error } = await supaStorage('valu.svg')
 */
const supaStorage = async ( dir ) => {
  const { data, error } = await supabase.storage.from('valu').download(dir)
  if (error) {
    console.error(error)
  }
  return data
}

export { supabase, supaStorage }
