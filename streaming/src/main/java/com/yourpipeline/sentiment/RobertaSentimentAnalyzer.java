package com.yourpipeline.sentiment;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;

import java.nio.LongBuffer;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Runs cardiffnlp/twitter-roberta-base-sentiment inference via DJL tokenizer
 * and ONNX Runtime Java API.
 *
 * DJL's TextClassificationTranslatorFactory creates uint32 tensors internally,
 * which OrtUtils does not support.  This class bypasses DJL's translator and
 * calls the ONNX Runtime Java API directly with explicit int64 (LongBuffer)
 * tensors
 *
 * Labels: 0 = Negative, 1 = Neutral, 2 = Positive
 */
public class RobertaSentimentAnalyzer implements AutoCloseable {

    private static final Map<Integer, String> LABELS = Map.of(
            0, "Negative",
            1, "Neutral",
            2, "Positive");

    private final HuggingFaceTokenizer tokenizer;
    private final OrtEnvironment       env;
    private final OrtSession           session;

    public RobertaSentimentAnalyzer(String modelDir) throws Exception {
        var dir      = Paths.get(modelDir);
        this.env     = OrtEnvironment.getEnvironment();

        // Explicitly limit ONNX Runtime thread pools to avoid saturating all CPU cores.
        // OMP_NUM_THREADS alone is not sufficient — SessionOptions controls the runtime's
        // own intra-op and inter-op thread pools independently of OpenMP.
        int threads = Integer.parseInt(System.getenv().getOrDefault("OMP_NUM_THREADS", "2"));
        OrtSession.SessionOptions opts = new OrtSession.SessionOptions();
        opts.setIntraOpNumThreads(threads);
        opts.setInterOpNumThreads(1);

        this.session = env.createSession(dir.resolve("model.onnx").toString(), opts);
        this.tokenizer = HuggingFaceTokenizer.builder()
                .optTokenizerPath(dir.resolve("tokenizer.json"))
                .optMaxLength(512)
                .optTruncation(true)
                .build();
    }

    public String analyze(String text) throws OrtException {
        Encoding enc   = tokenizer.encode(text);
        long[]   ids   = enc.getIds();
        long[]   mask  = enc.getAttentionMask();
        long[]   shape = {1L, ids.length};

        try (
            OnnxTensor inputIds      = OnnxTensor.createTensor(env, LongBuffer.wrap(ids),  shape);
            OnnxTensor attentionMask = OnnxTensor.createTensor(env, LongBuffer.wrap(mask), shape);
            OrtSession.Result result = session.run(Map.of(
                    "input_ids",      inputIds,
                    "attention_mask", attentionMask))
        ) {
            float[][] logits = (float[][]) result.get("logits").get().getValue();
            return LABELS.getOrDefault(argmax(logits[0]), "Unknown");
        }
    }

    private static int argmax(float[] arr) {
        int best = 0;
        for (int i = 1; i < arr.length; i++) {
            if (arr[i] > arr[best]) best = i;
        }
        return best;
    }

    @Override
    public void close() throws Exception {
        session.close();
        tokenizer.close();
        // OrtEnvironment is a global singleton — do not close it
    }
}
