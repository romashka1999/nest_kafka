import { Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { NestFactory } from '@nestjs/core';
import { DocumentBuilder, SwaggerModule } from '@nestjs/swagger';

import { AppModule } from './app.module';

async function bootstrap() {
    const logger = new Logger('bootstrap');

    const app = await NestFactory.create(AppModule);

    const configService = app.get(ConfigService);

    const swaggerConfig = new DocumentBuilder()
        .setTitle('Kafka')
        .setDescription('This is a kafka app.')
        .setVersion('1.0')
        .build();

    const document = SwaggerModule.createDocument(app, swaggerConfig);
    SwaggerModule.setup('docs', app, document);

    await app.listen(configService.get('HTTP_PORT'));

    const httpAppUrl = await app.getUrl();

    logger.verbose('='.repeat(50));
    logger.verbose(`http api listenig to: ${httpAppUrl}`);
    logger.verbose(`rest api docs: ${httpAppUrl}/docs`);
    logger.verbose('='.repeat(50));
}
bootstrap();
