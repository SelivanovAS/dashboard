"""Офлайн-проверки учёта ответов и ошибок перед платным запуском."""
import unittest
import httpx
import erknm_compare as compare

class CompareTests(unittest.IsolatedAsyncioTestCase):
    async def test_answer_and_cost(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={'choices': [{'message': {'content': 'AБ3О4'}, 'finish_reason': 'stop'}], 'usage': {'cost': 0.001, 'prompt_tokens': 100, 'completion_tokens': 5}}))) as client:
            row = await compare.answer(client, compare.MODELS[1], 'data:image/png;base64,AA==')
        self.assertEqual(row['code'], 'АБ3О4')
        self.assertEqual(row['cost'], 0.001)

    async def test_api_error_not_wrong_answer(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(429))) as client:
            row = await compare.answer(client, compare.MODELS[0], 'data:image/png;base64,AA==')
        self.assertEqual(row['http'], 429)
        self.assertNotIn('code', row)
        self.assertNotIn('cost', row)

    def test_normalize(self):
        self.assertEqual(compare.normalize('Ответ: A Б 3 О 4'), 'АБ3О4')

if __name__ == '__main__':
    unittest.main()
