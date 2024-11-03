<template>
    <Alert type="info">
        <div style="display:flex; flex-direction: column">
            <div style="display:flex">
                <div>
                    <Icon name="codicon:github-inverted" />
                </div>
                <div id="sample" style="padding-left: 1rem; display: flex; flex-direction: column">
                    <strong>
                        <NuxtLink :to="link" target="_blank" style="margin-right: 0.5rem;">{{title}}</NuxtLink>
                        <Icon name="heroicons:arrow-top-right-on-square" />
                    </strong>
                    <ContentSlot :use="$slots.default" unwrap="p" />
                </div>
            </div>
        </div>
    </Alert>
</template>

<script lang="ts" setup>

type Props = {
    link: string
    title: string
}
defineProps<Props>()

</script>

<style>
#sample div p {
    margin: 0;
    margin-top: 0.5rem;
}
</style>
