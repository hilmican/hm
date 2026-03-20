import 'package:google_mlkit_text_recognition/google_mlkit_text_recognition.dart';

/// Runs on-device Latin script OCR (Turkish etiketleri dahil).
Future<String> recognizeTextFromImagePath(String path) async {
  final input = InputImage.fromFilePath(path);
  final recognizer = TextRecognizer(script: TextRecognitionScript.latin);
  try {
    final recognized = await recognizer.processImage(input);
    return recognized.text;
  } finally {
    await recognizer.close();
  }
}
