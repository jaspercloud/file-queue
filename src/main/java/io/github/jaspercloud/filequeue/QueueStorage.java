package io.github.jaspercloud.filequeue;

import io.github.jaspercloud.filequeue.exception.NotDataException;
import io.github.jaspercloud.filequeue.exception.NotSpaceException;
import io.github.jaspercloud.filequeue.exception.QueueFileException;
import io.github.jaspercloud.filequeue.exception.WaitDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueueStorage {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private static final String FilePrefix = "log_";

    private File storageDir;
    private long maxSize;
    private AtomicLong idGen = new AtomicLong();
    private StorageLock logFileLock = new StorageLock();
    private CheckPointer readCheckPointer;
    private CheckPointer writeCheckPointer;
    private BlockingQueue<DBFile> DBFileQueue = new LinkedBlockingQueue<>();
    private AtomicReference<DBFile> writeLog;
    private ScheduledFuture<?> flushFuture;

    public QueueStorage(File storageDir, long maxSize, long flushTime) throws Exception {
        this.storageDir = storageDir;
        this.maxSize = maxSize;
        if (!storageDir.exists()) {
            storageDir.mkdirs();
        }
        init();

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
        //checkPointTask
        flushFuture = scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    saveCheckPoint();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }, 0, flushTime, TimeUnit.MILLISECONDS);
        //closeTask
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    saveCheckPoint();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
                try {
                    close();
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }));
    }

    private void init() throws Exception {
        readCheckPointer = new CheckPointer(new File(storageDir, "rMeta"));
        writeCheckPointer = new CheckPointer(new File(storageDir, "wMeta"));
        CheckPointer.CheckPoint readCheckPoint = readCheckPointer.get();
        CheckPointer.CheckPoint writeCheckPoint = writeCheckPointer.get();
        //scanFiles
        List<File> fileList = Arrays.asList(storageDir.listFiles()).stream()
                .filter(e -> {
                    //filter log file
                    return e.getName().startsWith(FilePrefix);
                })
                .filter(e -> {
                    if (null == readCheckPoint) {
                        return true;
                    }
                    Long checkNum = getFileNum(readCheckPoint.getName());
                    Long fileNum = getFileNum(e.getName());
                    if (fileNum >= checkNum) {
                        return true;
                    }
                    //delete old logFile
                    boolean delete = e.delete();
                    if (!delete) {
                        throw new QueueFileException("can't delete file: " + e.getName());
                    }
                    return false;
                })
                .sorted(Comparator.comparingLong(e -> {
                    return getFileNum(e.getName());
                }))
                .collect(Collectors.toList());

        //set idGen
        if (fileList.isEmpty()) {
            File file = createFile();
            fileList.add(file);
        } else {
            File file = fileList.get(fileList.size() - 1);
            long id = getFileNum(file.getName());
            idGen.set(id);
        }

        //setPos
        List<DBFile> collect = fileList.stream().map(e -> {
            try {
                DBFile DBFile = new DBFile(e, maxSize, logFileLock);
                MappedByteBuffer writeBuffer = DBFile.getWriter().getWriteBuffer();
                writeBuffer.position(writeBuffer.capacity());
                if (null != readCheckPoint && Objects.equals(readCheckPoint.getName(), e.getName())) {
                    DBFile.getReader().getReaderBuffer().position(readCheckPoint.getPos());
                }
                return DBFile;
            } catch (Exception ex) {
                throw new ExceptionInInitializerError(ex);
            }
        }).collect(Collectors.toList());

        if (collect.isEmpty()) {
            throw new QueueFileException();
        }
        DBFile writeDBFile = collect.get(collect.size() - 1);
        if (null != writeCheckPoint) {
            writeDBFile.getWriter().getWriteBuffer().position(writeCheckPoint.getPos());
        } else {
            writeDBFile.getWriter().getWriteBuffer().position(0);
        }

        writeLog = new AtomicReference<>(writeDBFile);
        DBFileQueue.addAll(collect);
    }

    private void saveCheckPoint() throws Exception {
        Lock readLock = readCheckPointer.getCheckPointLock();
        Lock writeLock = writeCheckPointer.getCheckPointLock();
        logger.debug("start saveCheckPoint");
        try {
            readLock.lock();
            writeLock.lock();
            {
                DBFile writeDBFile = writeLog.get();
                String name = writeDBFile.getFile().getName();
                int position = writeDBFile.getWriter().getWriteBuffer().position();
                writeDBFile.flush();
                writeCheckPointer.save(new CheckPointer.CheckPoint(name, position));
            }
            {
                DBFile readDBFile = takeReadLogFile();
                String name = readDBFile.getFile().getName();
                int position = readDBFile.getReader().getReaderBuffer().position();
                readCheckPointer.save(new CheckPointer.CheckPoint(name, position));
            }
        } finally {
            readLock.unlock();
            writeLock.unlock();
        }
        logger.debug("end saveCheckPoint");
    }

    public void put(byte[] bytes) throws Exception {
        ReentrantLock checkPointLock = writeCheckPointer.getCheckPointLock();
        try {
            logFileLock.getWriteLock().lock();
            boolean checkPointLocked = checkPointLock.isLocked();
            if (checkPointLocked) {
                logger.debug("put: waiting checkPointLock");
            }
            checkPointLock.lock();
            if (checkPointLocked) {
                logger.debug("put: acquire checkPointLock");
            }
            DBFile current = writeLog.get();
            try {
                current.putRecord(bytes);
            } catch (NotSpaceException e) {
                current.endFile();
                current.flush();
                current.getWriter().close();

                File file = createFile();
                DBFile DBFile = new DBFile(file, maxSize, logFileLock);
                DBFile.putRecord(bytes);
                writeLog.set(DBFile);
                DBFileQueue.offer(DBFile);
            }
            logFileLock.signalAll();
        } finally {
            checkPointLock.unlock();
            logFileLock.getWriteLock().unlock();
        }
    }

    public byte[] take() throws Exception {
        do {
            byte[] bytes = get();
            if (null != bytes) {
                return bytes;
            }
        } while (true);
    }

    public byte[] get() throws Exception {
        ReentrantLock checkPointLock = readCheckPointer.getCheckPointLock();
        try {
            logFileLock.getReadLock().lock();
            boolean checkPointLocked = checkPointLock.isLocked();
            if (checkPointLocked) {
                logger.debug("get: waiting checkPointLock");
            }
            checkPointLock.lock();
            if (checkPointLocked) {
                logger.debug("get: acquire checkPointLock");
            }
            try {
                DBFile DBFile = takeReadLogFile();
                byte[] bytes = DBFile.readRecord();
                return bytes;
            } catch (WaitDataException e) {
                try {
                    logFileLock.getWriteLock().lock();
                    logFileLock.await();
                } finally {
                    logFileLock.getWriteLock().unlock();
                }
            } catch (NotDataException e) {
                DBFile DBFile = DBFileQueue.poll();
                if (null != DBFile) {
                    DBFile.close();
                    boolean delete = DBFile.getFile().delete();
                    if (!delete) {
                        throw new QueueFileException("can't delete file: " + DBFile.getFile().getName());
                    }
                }
            }
            return null;
        } finally {
            checkPointLock.unlock();
            logFileLock.getReadLock().unlock();
        }
    }

    private File createFile() {
        long id = idGen.incrementAndGet();
        File file = new File(storageDir, FilePrefix + String.valueOf(id));
        return file;
    }

    private DBFile takeReadLogFile() throws Exception {
        do {
            DBFile DBFile = DBFileQueue.peek();
            if (null != DBFile) {
                return DBFile;
            }
            try {
                logFileLock.getWriteLock().lock();
                logFileLock.await();
            } finally {
                logFileLock.getWriteLock().unlock();
            }
        } while (true);
    }

    public void close() {
        Iterator<DBFile> iterator = DBFileQueue.iterator();
        while (iterator.hasNext()) {
            try {
                DBFile next = iterator.next();
                next.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        if (null != flushFuture) {
            flushFuture.cancel(true);
        }
    }

    public Long getFileNum(String name) {
        Pattern pattern = Pattern.compile("\\d+$");
        Matcher matcher = pattern.matcher(name);
        if (!matcher.find()) {
            throw new QueueFileException();
        }
        String group = matcher.group();
        long num = Long.parseLong(group);
        return num;
    }
}
