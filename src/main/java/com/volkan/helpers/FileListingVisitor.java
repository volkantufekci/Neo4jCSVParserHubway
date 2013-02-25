package com.volkan.helpers;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Recursive listing with SimpleFileVisitor in JDK 7. */
public final class FileListingVisitor {

	public static void main(String... aArgs) throws IOException {
		String ROOT = "/home/volkan/Documents/workspace-sts-2.7.2.RELEASE/" +
				"neowithmaven20121016/src/main/resources/jsons/load_test";
//		List<String> fileNames = listJsonFileNamesInDir(ROOT);
//		for (String fileName : fileNames) {
//			System.out.println(fileName);
//		}
		
		List<String> fileNames = listRandomJsonFileNamesInDir(ROOT, 4);
		for (String fileName : fileNames) {
			System.out.println(fileName);
		}
	}

	public static String randomJsonFileNameFromDir(String rootDir) throws IOException {
		
		List<String> fileNames = listJsonFileNamesInDir(rootDir);
		
		Random random = new Random();
		int randomNumber = random.nextInt(fileNames.size());

		return fileNames.get(randomNumber);
	}
	
	public static List<String> listRandomJsonFileNamesInDir(String rootDir, int totalRandomCount)
			throws IOException {
		
		List<String> fileNames = listJsonFileNamesInDir(rootDir);
		assert fileNames.size() >= totalRandomCount : "Requested randomCount could not be bigger" +
				" than fileNames size";
		
		List<String> randomFileNames = new ArrayList<>();
		
		for (int i = 0; i < totalRandomCount; i++) {
			Random random = new Random();
			int randomNumber = random.nextInt(totalRandomCount);
			randomFileNames.add(fileNames.get(randomNumber));
		}
		
		return randomFileNames;
	}
	
	public static List<String> listJsonFileNamesInDir(String rootDir)
			throws IOException {
		FileListingVisitor fl = new FileListingVisitor();
		ProcessFile pf = fl.new ProcessFile();
		FileVisitor<Path> fileProcessor = pf; 
		Files.walkFileTree(Paths.get(rootDir), fileProcessor);
		
		List<String> fileNames = pf.fileNames;
		return fileNames;
	}

	private final class ProcessFile extends SimpleFileVisitor<Path> {
		public  List<String> fileNames = new ArrayList<>();
		
		@Override
		public FileVisitResult visitFile(Path aFile, BasicFileAttributes aAttrs)
				throws IOException {
//			System.out.println("Processing file:" + aFile.getFileName());
			String fileName = aFile.getFileName().toString();
			if (fileName.endsWith(".json")) {
				fileNames.add( aFile.toString() );
			}
			return FileVisitResult.CONTINUE;
		}

		@Override
		public FileVisitResult preVisitDirectory(Path aDir,
				BasicFileAttributes aAttrs) throws IOException {
			System.out.println("Processing directory:" + aDir);
			return FileVisitResult.CONTINUE;
		}
	}
}